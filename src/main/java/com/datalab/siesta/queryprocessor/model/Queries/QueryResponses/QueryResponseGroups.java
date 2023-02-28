package com.datalab.siesta.queryprocessor.model.Queries.QueryResponses;

import com.datalab.siesta.queryprocessor.model.GroupOccurrences;

import java.util.List;

public class QueryResponseGroups implements QueryResponse{

    private List<GroupOccurrences> occurrences;

    public QueryResponseGroups() {
    }

    public List<GroupOccurrences> getOccurrences() {
        return occurrences;
    }

    public void setOccurrences(List<GroupOccurrences> occurrences) {
        this.occurrences = occurrences;
    }
}
