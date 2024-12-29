package com.datalab.siesta.queryprocessor.declare.queryResponses;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class QueryResponseDeclareAllState extends QueryResponseDeclareAll{

    @JsonProperty("state up to date")
    private boolean upToDate;

    @JsonProperty("traces percentage on state")
    private double tracesPercentage;

    @JsonProperty("events percentage on state")
    private double eventsPercentage;

    @JsonProperty("message")
    private String message;

    public QueryResponseDeclareAllState(QueryResponsePosition queryResponsePosition, QueryResponseExistence queryResponseExistence, 
    QueryResponseOrderedRelations simple, QueryResponseOrderedRelations alternate, QueryResponseOrderedRelations chain) {
        super(queryResponseExistence, queryResponsePosition, simple, alternate, simple);
    }

}
