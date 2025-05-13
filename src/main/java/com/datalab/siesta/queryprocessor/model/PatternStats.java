package com.datalab.siesta.queryprocessor.model;

import com.amazonaws.services.iot1clickprojects.model.DeviceTemplate;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PatternStats {
    public double mean;
    public double std;
    List<Long> durations;

    public PatternStats(double mean, double std, List<Long> durations) {
        this.mean = mean;
        this.std = std;
        this.durations = durations;
    }

    // Calculate mean and standard deviation for each pattern
    public static Map<String, PatternStats> calculateStatistics(Map<String, List<Long>> patterns) {
        Map<String, PatternStats> statsMap = new HashMap<>();

        for (Map.Entry<String, List<Long>> entry : patterns.entrySet()) {
            List<Long> durations = entry.getValue();
            double mean = durations.stream().mapToDouble(Long::doubleValue).average().orElse(0.0);
            double std = Math.sqrt(durations.stream().mapToDouble(d -> Math.pow(d - mean, 2)).average().orElse(0.0));

            statsMap.put(entry.getKey(), new PatternStats(mean, std, durations));
        }

        return statsMap;
    }

    public static class Deviation {
        long duration;
        double deviation;
        boolean isViolated;

        public Deviation(long duration, double deviation, boolean isViolated) {
            this.duration = duration;
            this.deviation = deviation;
            this.isViolated = isViolated;
        }

        @Override
        public String toString() {
            return String.format("Duration: %d, Deviation: %.2f, Violated: %b", duration, deviation, isViolated);
        }

        public static Map<String, Double> calculateDeviations(Map<String, PatternStats> statsL1,
                                                                       Map<String, List<Long>> patternsL2,
                                                                       double factor) {
            Map<String, List<Deviation>> deviations = new HashMap<>();

            for (Map.Entry<String, List<Long>> entry : patternsL2.entrySet()) {
                String pattern = entry.getKey();
                List<Long> durationsL2 = entry.getValue();

                if (statsL1.containsKey(pattern)) {
                    double meanL1 = statsL1.get(pattern).mean;
                    double stdL1 = statsL1.get(pattern).std;
                    for (long durationL2 : durationsL2) {
                        double deviation = durationL2 - meanL1;
                        boolean isViolated = Math.abs(deviation) > factor * stdL1;
                        deviations.putIfAbsent(pattern, new ArrayList<>());
                        deviations.get(pattern).add(new Deviation(durationL2, deviation, isViolated));
                    }
                }
            }

            return getViolations(deviations);
        }

        private static @NotNull Map<String, Double> getViolations(Map<String, List<Deviation>> deviations) {
            Map<String, Double> violations = new HashMap<>();

            for (Map.Entry<String, List<Deviation>> entry: deviations.entrySet()) {
                long countViolations = 0;
                double mean = 0;
                for (Deviation dev : entry.getValue()) {
                    mean += dev.duration;
                    if (dev.isViolated)
                        countViolations++;
                }
                if (countViolations < Math.ceil((double) entry.getValue().size() / 2))
                    violations.put(entry.getKey(), mean / entry.getValue().size());
            }
            return violations;
        }
    }

}
