package com.datalab.siesta.queryprocessor.model.Queries.QueryResponses;

import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Occurrences;

import java.util.List;
import java.util.Map;

public class QueryResponsePatternDetection implements QueryResponse {

    private List<Occurrences> occurrences;

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
}
