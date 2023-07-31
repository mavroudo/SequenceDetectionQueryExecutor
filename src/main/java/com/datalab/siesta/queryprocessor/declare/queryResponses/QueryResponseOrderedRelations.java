package com.datalab.siesta.queryprocessor.declare.queryResponses;

import com.datalab.siesta.queryprocessor.declare.model.EventPairSupport;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class QueryResponseOrderedRelations implements QueryResponse {

    private String mode;
    private List<EventPairSupport> response;
    private List<EventPairSupport> precedence;
    private List<EventPairSupport> succession;
    @JsonProperty("not-succession")
    private List<EventPairSupport> notSuccession;

    public QueryResponseOrderedRelations() {
    }

    public QueryResponseOrderedRelations(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public List<EventPairSupport> getResponse() {
        return response;
    }

    public void setResponse(List<EventPairSupport> response) {
        this.response = response;
    }

    public List<EventPairSupport> getPrecedence() {
        return precedence;
    }

    public void setPrecedence(List<EventPairSupport> precedence) {
        this.precedence = precedence;
    }

    public List<EventPairSupport> getSuccession() {
        return succession;
    }

    public void setSuccession(List<EventPairSupport> succession) {
        this.succession = succession;
    }

    public List<EventPairSupport> getNotSuccession() {
        return notSuccession;
    }

    public void setNotSuccession(List<EventPairSupport> notSuccession) {
        this.notSuccession = notSuccession;
    }
}
