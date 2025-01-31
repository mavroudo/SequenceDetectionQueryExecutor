package com.datalab.siesta.queryprocessor.model.Queries.QueryResponses;

import com.datalab.siesta.queryprocessor.model.GroupOccurrences;
import com.datalab.siesta.queryprocessor.model.TimeStats;
import com.fasterxml.jackson.annotation.JsonProperty;


import java.util.List;

/**
 * The response for the pattern detection query when groups were used. It contains a list of the occurrences per
 * group and information about the time required for each step of the execution (pruning and validation)
 */
public class QueryResponseGroups implements QueryResponse{

    private List<GroupOccurrences> occurrences;

    @JsonProperty("performance statistics")
    private TimeStats timeStats;

    public QueryResponseGroups() {
    }

    public List<GroupOccurrences> getOccurrences() {
        return occurrences;
    }

    public void setOccurrences(List<GroupOccurrences> occurrences) {
        this.occurrences = occurrences;
    }

    public TimeStats getTimeStats() {
        return timeStats;
    }

    public void setTimeStats(TimeStats timeStats) {
        this.timeStats = timeStats;
    }
}
