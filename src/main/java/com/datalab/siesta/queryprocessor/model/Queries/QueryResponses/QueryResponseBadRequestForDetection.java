package com.datalab.siesta.queryprocessor.model.Queries.QueryResponses;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.ArrayList;
import java.util.List;

@JsonInclude(Include.NON_EMPTY)
public class QueryResponseBadRequestForDetection implements QueryResponse {

    @JsonProperty("Pair of events that do not exist")
    protected List<EventPair> nonExistingPairs;

    @JsonProperty("Constraints between events that cannot been fulfilled")
    protected List<EventPair> constraintsNotFulfilled;

    @JsonProperty("Events dont exist in the db")
    protected List<String> nonExistingEvents;

    @JsonProperty("Constraints containing errors")
    protected List<Constraint> wrongConstraints;

    public QueryResponseBadRequestForDetection(){
        nonExistingPairs=new ArrayList<>();
        constraintsNotFulfilled=new ArrayList<>();
        nonExistingEvents=new ArrayList<>();
        wrongConstraints=new ArrayList<>();
    }

    @JsonIgnore
    public boolean isEmpty(){
        return nonExistingEvents.isEmpty() && constraintsNotFulfilled.isEmpty() && nonExistingPairs.isEmpty() &&
                wrongConstraints.isEmpty();
    }

    public List<Constraint> getWrongConstraints() {
        return wrongConstraints;
    }

    public void setWrongConstraints(List<Constraint> wrongConstraints) {
        this.wrongConstraints = wrongConstraints;
    }

    public List<String> getNonExistingEvents() {
        return nonExistingEvents;
    }

    public void setNonExistingEvents(List<String> nonExistingEvents) {
        this.nonExistingEvents = nonExistingEvents;
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
