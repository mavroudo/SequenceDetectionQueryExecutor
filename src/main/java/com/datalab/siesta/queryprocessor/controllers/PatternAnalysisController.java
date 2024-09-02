package com.datalab.siesta.queryprocessor.controllers;


import com.amazonaws.thirdparty.jackson.core.type.TypeReference;
import com.amazonaws.thirdparty.jackson.databind.ObjectMapper;
import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.DBModel.EventTypes;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexPair;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexRecords;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.PatternStats;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import com.nimbusds.jose.shaded.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.datalab.siesta.queryprocessor.model.PatternStats.Deviation.calculateDeviations;
import static com.datalab.siesta.queryprocessor.model.PatternStats.calculateStatistics;

/**
 * TODO: description
 */
@RestController
@RequestMapping(path = "/patterns")
public class PatternAnalysisController {

    @Autowired
    private DBConnector dbConnector;

    @RequestMapping(path = "/violations", method = RequestMethod.POST, consumes = "multipart/form-data")
    public ResponseEntity<JSONObject> getViolatingPatterns(@RequestParam("config") MultipartFile configFile) {

        JSONObject response = new JSONObject();

        List<String> legitimates;

        String log_database = "test"; // Default value
        double std_factor = 1.5;      // Default value
        boolean split_on_resource = false;  // Default value
        double pair_support = 0.5;    // Default value
        double diverging_index_factor = 0.7; // Default value

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> jsonMap = objectMapper.readValue(configFile.getInputStream(), new TypeReference<Map<String, Object>>() {});

            // Parse values with default fallbacks
            if (jsonMap.containsKey("log_database")) {
                log_database = (String) jsonMap.get("log_database");
            }
            if (jsonMap.containsKey("std_factor")) {
                std_factor = ((Number) jsonMap.get("std_factor")).doubleValue();
            }
            if (jsonMap.containsKey("split_on_resource")) {
                split_on_resource = (Boolean) jsonMap.get("split_on_resource");
            }
            if (jsonMap.containsKey("pair_support")) {
                pair_support = ((Number) jsonMap.get("pair_support")).doubleValue();
            }
            if (jsonMap.containsKey("diverging_index_factor")) {
                diverging_index_factor = ((Number) jsonMap.get("diverging_index_factor")).doubleValue();
            }
            if (jsonMap.containsKey("legitimate_ids")) {
                legitimates = (List<String>) jsonMap.get("legitimate_ids");
            }
            else {
                response.put("error", "Missing legitimate_ids in JSON file");
                return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);
            }
        } catch (IOException e) {
            response.put("error", "Failed to parse JSON file");
            return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);
        }

        var log = splitLogInstances(log_database, legitimates, pair_support, diverging_index_factor);

        List<IndexPair> l1 = log._1;
        List<IndexPair> l2 = log._2;

        // Extract patterns and calculate statistics
        Map<String, List<Long>> patternsL1 = extractPatterns(l1);
        Map<String, List<Long>> patternsL2 = extractPatterns(l2);

        Map<String, PatternStats> statsL1 = calculateStatistics(patternsL1);

        // Print results
        var violations = getViolatingPatterns(statsL1, calculateDeviations(statsL1, patternsL2, std_factor));

        return new ResponseEntity<>(violations, HttpStatus.OK);
    }

    public static JSONObject getViolatingPatterns(Map<String, PatternStats> statsL1, Map<String, Double> violations) {
        JSONObject resultJson = new JSONObject();

        for (Map.Entry<String, PatternStats> entry : statsL1.entrySet()) {
            JSONObject patternData = new JSONObject();
            patternData.put("L1_Mean", entry.getValue().mean);
            patternData.put("L2_Mean", violations.get(entry.getKey()));

            resultJson.put(entry.getKey(), patternData);
        }

        return resultJson;
    }

    private Tuple2<List<IndexPair>, List<IndexPair>> splitLogInstances(String log_database, List<String> legitimateTraces, double pair_support, double diverging_factor) {

        // Get num of traces in log
        long tracesNum = dbConnector.getMetadata(log_database).getTraces();

        // Get all event pairs found in log
        List<Count> pairs = dbConnector.getEventPairs(log_database);

        // Filter pairs to keep only frequent ones
        List<Count> freqPairs = pairs.stream()
                .filter(pair -> pair.getCount() >= pair_support * tracesNum)
                .toList();

        // Filter pairs to keep those with great variance (based on CV calculation)
        List<Count> divergingPairs = getDivergingPairs(freqPairs, diverging_factor);

        // Get all the event pair instances from the Index table
        Set<EventPair> pairsSet = new HashSet<>();
        for (Count pair : divergingPairs) {
            pairsSet.add(new EventPair(new Event(pair.getEventA()), new Event(pair.getEventB())));
        }
        Map<EventTypes, List<IndexPair>> indexRecords =
                dbConnector.queryIndexTable(pairsSet, log_database, dbConnector.getMetadata(log_database), null, null).getRecords();

        // Split pair instances into 2 lists;
        List<IndexPair> legitimateInstances = new ArrayList<>();
        List<IndexPair> illegitimateInstances = new ArrayList<>();
        indexRecords.forEach((et, instances) -> instances.forEach(instance -> {
            if (legitimateTraces.contains(instance.getTraceId())) {
                legitimateInstances.add(instance);
            } else {
                illegitimateInstances.add(instance);
            }
        }));
        return new Tuple2<>(legitimateInstances, illegitimateInstances);
    }

    /**
     * Calculates the coefficient of variation (CV) of a pair
     * @param pair an entry of CountTable
     * @return CV of the pair's duration
     */
    private double getCV(Count pair) {
        double norm_factor = 1.0 / pair.getCount();
        double mean = (double) pair.getSum_duration() / pair.getCount();
        double var = norm_factor * (pair.getSum_squares() - norm_factor * Math.pow(pair.getSum_duration(), 2));
        return var / mean;
    }

    private List<Count> getDivergingPairs(List<Count> pairs, double factor) {
        List<Count> sortedPairs = new ArrayList<>(pairs);
        sortedPairs.sort((o1, o2) -> Double.compare(getCV(o2), getCV(o1)));
        return sortedPairs.subList(0, (int) Math.ceil(factor * pairs.size()));
    }

    public static Map<String, List<Long>> extractPatterns(List<IndexPair> log) {
        Map<String, List<Long>> patterns = new HashMap<>();

        for (IndexPair pair : log) {
            String patternKey = pair.getEventA() + pair.getEventB();
            patterns.putIfAbsent(patternKey, new ArrayList<>());
            patterns.get(patternKey).add(pair.getDuration());
        }

        return patterns;
    }
    /*
    public static String printViolatingPatterns(Map<String, Tuple2<PatternStats, PatternStats>> o) {
        StringBuilder result = new StringBuilder();
        result.append("Patterns frequently violated in L2:\n");

        for (Map.Entry<String, Tuple2<PatternStats, PatternStats>> entry : o.entrySet()) {
            result.append(String.format("Pattern %s: Mean_L1 = %.4f, Mean_L2 = %.4f, Var_L1= %.4f, Var_L2 = %.4f\n",
                    entry.getKey(), entry.getValue()._1.mean, entry.getValue()._2.mean, entry.getValue()._1.std, entry.getValue()._2.std));
        }
        return result.toString();
    }

    public static Map<String, Tuple2<PatternStats, PatternStats>> getViolatingPatterns(Map<String, PatternStats> statsL1, Map<String, PatternStats> statsL2, double stdThreshold) {
        Map<String, Tuple2<PatternStats, PatternStats>> violatingPatterns = new HashMap<>();

        for (Map.Entry<String, PatternStats> entry : statsL2.entrySet()) {
            String pattern = entry.getKey();
            PatternStats statsL2Pattern = entry.getValue();

            if (statsL1.containsKey(pattern)) {
                PatternStats statsL1Pattern = statsL1.get(pattern);

                // Check if the deviation exceeds the threshold
                if (Math.abs(statsL2Pattern.mean - statsL1Pattern.mean) > stdThreshold * statsL1Pattern.std) {
                    violatingPatterns.putIfAbsent(pattern, new Tuple2<>(statsL1Pattern,statsL2Pattern));
                }
            }
        }
        return violatingPatterns;
    }
    */



}
