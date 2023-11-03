package com.datalab.siesta.queryprocessor.model.Queries.QueryResponses;


import com.datalab.siesta.queryprocessor.model.Occurrences;
import com.datalab.siesta.queryprocessor.model.TimeStats;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 *  The response for the pattern detection query. It contains a list of the occurrences per
 *  trace and information about the time required for each step of the execution (pruning and validation)
 */
public class QueryResponsePatternDetection implements QueryResponse {

    protected List<Occurrences> occurrences;

    @JsonProperty("performance statistics")
    protected TimeStats timeStats;

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
