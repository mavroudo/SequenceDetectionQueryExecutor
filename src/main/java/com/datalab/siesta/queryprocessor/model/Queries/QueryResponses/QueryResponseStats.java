package com.datalab.siesta.queryprocessor.model.Queries.QueryResponses;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;

import java.util.List;

/**
 * The response for the stats query. Contains a list of the retrieved stats (from the CountTable) for each consecutive
 * et-pair in the query pattern.
 */
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
