package com.datalab.siesta.queryprocessor.declare.queryResponses;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class QueryResponseOrderedRelationsState extends QueryResponseOrderedRelations{

    @JsonProperty("state up to date")
    private boolean upToDate;

    @JsonProperty("traces percentage on state")
    private double tracesPercentage;

    @JsonProperty("events percentage on state")
    private double eventsPercentage;

    @JsonProperty("message")
    private String message;

}
