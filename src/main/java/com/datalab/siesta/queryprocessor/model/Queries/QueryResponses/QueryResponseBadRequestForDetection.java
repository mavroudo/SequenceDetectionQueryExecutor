package com.datalab.siesta.queryprocessor.model.Queries.QueryResponses;

import com.datalab.siesta.queryprocessor.model.EventPair;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.ArrayList;
import java.util.List;

@JsonInclude(Include.NON_EMPTY)
public class QueryResponseBadRequestForDetection implements QueryResponse {

    @JsonProperty("Pair of events that do not exist")
    private List<EventPair> nonExistingPairs;

    @JsonProperty("Constraints between events that cannot been fulfilled")
    private List<EventPair> constraintsNotFulfilled;

    public QueryResponseBadRequestForDetection(){
        nonExistingPairs=new ArrayList<>();
        constraintsNotFulfilled=new ArrayList<>();
    }

    public List<EventPair> getNonExistingPairs() {
        return nonExistingPairs;
    }

    public void setNonExistingPairs(List<EventPair> nonExistingPairs) {
        this.nonExistingPairs = nonExistingPairs;
    }

    public List<EventPair> getConstraintsNotFulfilled() {
        return constraintsNotFulfilled;
    }

    public void setConstraintsNotFulfilled(List<EventPair> constraintsNotFulfilled) {
        this.constraintsNotFulfilled = constraintsNotFulfilled;
    }
}
