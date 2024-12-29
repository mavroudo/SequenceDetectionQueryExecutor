package com.datalab.siesta.queryprocessor.declare.queryResponses;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Setter;
import lombok.Getter;

@Getter
@Setter
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

    @JsonIgnore
    public QueryResponsePosition getQueryResponsePosition(){
        QueryResponsePosition queryResponsePosition = new QueryResponsePosition();
        queryResponsePosition.setFirst(this.getFirst());
        queryResponsePosition.setLast(this.getLast());
        return queryResponsePosition;
    }

}
