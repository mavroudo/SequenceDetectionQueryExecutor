package com.datalab.siesta.queryprocessor.declare.queryResponses;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude;

@Getter
@Setter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_EMPTY) // Exclude null or empty fields
public class QueryResponseExistenceState extends QueryResponseExistence{

    @JsonProperty("state up to date")
    private boolean upToDate;

    @JsonProperty("traces percentage on state")
    private double tracesPercentage;

    @JsonProperty("events percentage on state")
    private double eventsPercentage;

    @JsonProperty("message")
    private String message;

}
