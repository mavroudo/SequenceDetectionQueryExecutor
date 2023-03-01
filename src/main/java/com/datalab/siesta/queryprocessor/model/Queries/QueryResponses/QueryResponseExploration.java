package com.datalab.siesta.queryprocessor.model.Queries.QueryResponses;

import com.datalab.siesta.queryprocessor.model.Proposition;

import java.util.ArrayList;
import java.util.List;

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
