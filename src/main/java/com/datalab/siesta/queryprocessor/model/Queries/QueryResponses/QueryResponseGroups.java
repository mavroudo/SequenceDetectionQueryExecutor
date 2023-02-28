package com.datalab.siesta.queryprocessor.model.Queries.QueryResponses;

import com.datalab.siesta.queryprocessor.model.GroupOccurrences;
import com.datalab.siesta.queryprocessor.model.TimeStats;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

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
