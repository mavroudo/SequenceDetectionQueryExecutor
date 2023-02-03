package com.datalab.siesta.queryprocessor.model.Queries.QueryResponses;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.EventPair;

import java.util.List;
import java.util.Map;

public class QueryResponseStats implements QueryResponse{

    List<Count>   counts;

    public List<Count> getCounts() {

        return counts;
    }

    public void setCounts(List<Count> counts) {
        this.counts = counts;
    }

    public QueryResponseStats(List<Count> counts) {
        this.counts = counts;
    }

    public QueryResponseStats() {
    }
}
