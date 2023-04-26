package com.datalab.siesta.queryprocessor.model.Queries.QueryResponses;


import com.datalab.siesta.queryprocessor.model.Occurrences;
import com.datalab.siesta.queryprocessor.model.TimeStats;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;


public class QueryResponsePatternDetection implements QueryResponse {

    private List<Occurrences> occurrences;

    @JsonProperty("performance statistics")
    private TimeStats timeStats;

    public QueryResponsePatternDetection() {
    }

    public QueryResponsePatternDetection(List<Occurrences> occurrences) {
        this.occurrences = occurrences;
    }

    public List<Occurrences> getOccurrences() {
        return occurrences;
    }

    public void setOccurrences(List<Occurrences> occurrences) {
        this.occurrences = occurrences;
    }

    public TimeStats getTimeStats() {
        return timeStats;
    }

    public void setTimeStats(TimeStats timeStats) {
        this.timeStats = timeStats;
    }
}
