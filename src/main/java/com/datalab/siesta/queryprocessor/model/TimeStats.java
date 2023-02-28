package com.datalab.siesta.queryprocessor.model;

import org.codehaus.jackson.annotate.JsonProperty;

public class TimeStats {

    @JsonProperty("time for pruning in ms")
    private long timeForPrune;

    @JsonProperty("time for validation in ms")
    private long timeForValidation;

    @JsonProperty("response time in ms")
    private long totalTime;

    public TimeStats() {
    }

    public long getTimeForPrune() {
        return timeForPrune;
    }

    public void setTimeForPrune(long timeForPrune) {
        this.timeForPrune = timeForPrune;
    }

    public long getTimeForValidation() {
        return timeForValidation;
    }

    public void setTimeForValidation(long timeForValidation) {
        this.timeForValidation = timeForValidation;
    }

    public long getTotalTime() {
        return totalTime;
    }

    public void setTotalTime(long totalTime) {
        this.totalTime = totalTime;
    }
}
