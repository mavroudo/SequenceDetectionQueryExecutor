package com.datalab.siesta.queryprocessor.declare.queryResponses;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Setter;
import lombok.Getter;

@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_EMPTY) // Exclude null or empty fields
public class QueryResponsePositionState extends QueryResponsePosition{

    @JsonProperty("state up to date")
    private boolean upToDate;

    @JsonProperty("traces percentage on state")
    private double tracesPercentage;

    @JsonProperty("events percentage on state")
    private double eventsPercentage;

    @JsonProperty("message")
    private String message;

    public QueryResponsePositionState(){}


}
