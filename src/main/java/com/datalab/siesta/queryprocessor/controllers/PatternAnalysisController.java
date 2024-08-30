package com.datalab.siesta.queryprocessor.controllers;


import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

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
                                                       @RequestParam(required = false, defaultValue = "0.95") double pair_support,
                                                       @RequestParam(required = false, defaultValue = "0.6") double cv_threshold) {
        // Get num of traces in log
        long tracesNum = dbConnector.getMetadata(log_database).getTraces();

        // Get all event pairs found in log
        List<Count> pairs = dbConnector.getEventPairs(log_database);

        // Filter pairs to keep only frequent ones
        List<Count> freqPairs = pairs.stream()
                .filter(pair -> pair.getCount() >= pair_support * tracesNum)
                .toList();

       // Filter pairs to keep those with great variance (based on CV calculation)
        List<Count> divergingPairs = pairs.stream()
                .filter(pair -> hasGreatVariance(pair, cv_threshold))
                .toList();

        System.out.println(divergingPairs);


        return new ResponseEntity<>("", HttpStatus.OK);
    }

    /**
     * Calculates the coefficient of variation (CV) of a pair and responds if it overcomes a given threshold
     * @param pair an entry of CountTable
     * @param cv_thr a CV threshold in [0-1], under which a pair's duration is considered stable
     * @return true if the pair's duration is considered unstable
     */
    private boolean hasGreatVariance(Count pair, double cv_thr) {
         long norm_factor = 1 / pair.getCount();
         double mean = (double) pair.getSum_duration() / pair.getCount();
         double var = norm_factor * (pair.getSum_squares() - norm_factor * Math.pow(pair.getSum_duration(), 2));
         return var / mean >= cv_thr;
    }
}
