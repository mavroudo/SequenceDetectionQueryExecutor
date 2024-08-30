package com.datalab.siesta.queryprocessor.controllers;


import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.DBModel.EventTypes;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexPair;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexRecords;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.PatternStats;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import scala.Tuple2;

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

    @RequestMapping(path = "/violations",method = RequestMethod.GET)
    public ResponseEntity<String> getViolatingPatterns(@RequestParam(required = false, defaultValue = "test") String log_database,
                                                       @RequestParam(defaultValue = "1") String legitimates,
                                                       @RequestParam(required = false, defaultValue = "1.2") double time_var_threshold_factor,
//                                                       @RequestParam(required = false, defaultValue = "false") boolean split_on_resource,
                                                       @RequestParam(required = false, defaultValue = "0.95") double pair_support,
                                                       @RequestParam(required = false, defaultValue = "0.7") double diverging_index_factor) {

        var log = splitLogInstances(log_database, legitimates, pair_support, diverging_index_factor);

        List<IndexPair> l1 = log._1;
        List<IndexPair> l2 = log._2;

        // Extract patterns and calculate statistics
        Map<String, List<Long>> patternsL1 = extractPatterns(l1);
        Map<String, List<Long>> patternsL2 = extractPatterns(l2);

        Map<String, PatternStats> statsL1 = calculateStatistics(patternsL1);
        Map<String, PatternStats> statsL2 = calculateStatistics(patternsL2);

        System.out.println(printViolatingPatterns(getViolatingPatterns(statsL1, statsL2, time_var_threshold_factor)));

        // Print extended results
        Map<String, List<PatternStats.Deviation>> deviations = calculateDeviations(statsL1, patternsL2, 1800);
        printExtendedResults(statsL1, deviations);

        return new ResponseEntity<>("Check terminal", HttpStatus.OK);
    }

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

    private static void printExtendedResults(Map<String, PatternStats> statsL1, Map<String, List<PatternStats.Deviation>> deviations) {
        for (Map.Entry<String, PatternStats> entry : statsL1.entrySet()) {
            System.out.println("-----Extended results-----\nPattern " + entry.getKey() + ": Mean = " + entry.getValue().mean + ", Std = " + entry.getValue().std);
        }

        System.out.println("\nDeviations in L2:");
        for (Map.Entry<String, List<PatternStats.Deviation>> entry : deviations.entrySet()) {
            System.out.println("\nPattern " + entry.getKey() + ":");
            for (PatternStats.Deviation deviation : entry.getValue()) {
                System.out.println(deviation);
            }
        }
    }

    private Tuple2<List<IndexPair>, List<IndexPair>> splitLogInstances(String log_database, String legitimates, double pair_support, double diverging_factor) {
        // Parse the trace ids that correspond to the legitimate process instances
        List<String> legitimateTraces =  Arrays.asList(legitimates.split(","));


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
}
