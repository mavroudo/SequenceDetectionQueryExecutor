package com.datalab.siesta.queryprocessor.declare.queryResponses;


import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryResponseDeclareAll implements QueryResponse{

    @JsonProperty("existence patterns")
    private QueryResponseExistence queryResponseExistence;

    @JsonProperty("position patterns")
    private QueryResponsePosition queryResponsePosition;

    @JsonProperty("ordered relations")
    private QueryResponseOrderedRelations queryResponseOrderedRelations;

    @JsonProperty("ordered relations alternate")
    private QueryResponseOrderedRelations queryResponseOrderedRelationsAlternate;

    @JsonProperty("ordered relations chain")
    private QueryResponseOrderedRelations queryResponseOrderedRelationsChain;

    public QueryResponseDeclareAll(QueryResponseExistence queryResponseExistence, QueryResponsePosition queryResponsePosition,
                            QueryResponseOrderedRelations queryResponseOrderedRelations, QueryResponseOrderedRelations
                                    queryResponseOrderedRelationsAlternate, QueryResponseOrderedRelations
                                    queryResponseOrderedRelationsChain) {
        this.queryResponseExistence = queryResponseExistence;
        this.queryResponsePosition = queryResponsePosition;
        this.queryResponseOrderedRelations = queryResponseOrderedRelations;
        this.queryResponseOrderedRelationsAlternate = queryResponseOrderedRelationsAlternate;
        this.queryResponseOrderedRelationsChain = queryResponseOrderedRelationsChain;
    }

    public QueryResponseExistence getQueryResponseExistence() {
        return queryResponseExistence;
    }

    public void setQueryResponseExistence(QueryResponseExistence queryResponseExistence) {
        this.queryResponseExistence = queryResponseExistence;
    }

    public QueryResponsePosition getQueryResponsePosition() {
        return queryResponsePosition;
    }

    public void setQueryResponsePosition(QueryResponsePosition queryResponsePosition) {
        this.queryResponsePosition = queryResponsePosition;
    }

    public QueryResponseOrderedRelations getQueryResponseOrderedRelations() {
        return queryResponseOrderedRelations;
    }

    public void setQueryResponseOrderedRelations(QueryResponseOrderedRelations queryResponseOrderedRelations) {
        this.queryResponseOrderedRelations = queryResponseOrderedRelations;
    }

    public QueryResponseOrderedRelations getQueryResponseOrderedRelationsAlternate() {
        return queryResponseOrderedRelationsAlternate;
    }

    public void setQueryResponseOrderedRelationsAlternate(QueryResponseOrderedRelations queryResponseOrderedRelationsAlternate) {
        this.queryResponseOrderedRelationsAlternate = queryResponseOrderedRelationsAlternate;
    }

    public QueryResponseOrderedRelations getQueryResponseOrderedRelationsChain() {
        return queryResponseOrderedRelationsChain;
    }

    public void setQueryResponseOrderedRelationsChain(QueryResponseOrderedRelations queryResponseOrderedRelationsChain) {
        this.queryResponseOrderedRelationsChain = queryResponseOrderedRelationsChain;
    }
}
