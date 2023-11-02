package com.datalab.siesta.queryprocessor.model.Queries.QueryResponses;

import com.datalab.siesta.queryprocessor.model.Proposition;

import java.util.ArrayList;
import java.util.List;

/**
 * The response for all the exploration queries. It contains a list of propositions of the most probable next event
 * for the query pattern.
 */
public class QueryResponseExploration implements QueryResponse{

    private List<Proposition> propositions;

    public QueryResponseExploration() {
        propositions=new ArrayList<>();
    }

    public QueryResponseExploration(List<Proposition> propositions) {
        this.propositions = propositions;
    }

    public List<Proposition> getPropositions() {
        return propositions;
    }

    public void setPropositions(List<Proposition> propositions) {
        this.propositions = propositions;
    }
}
