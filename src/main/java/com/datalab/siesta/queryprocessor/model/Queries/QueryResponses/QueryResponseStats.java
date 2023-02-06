package com.datalab.siesta.queryprocessor.model.Queries.QueryResponses;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;

import java.util.List;

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
