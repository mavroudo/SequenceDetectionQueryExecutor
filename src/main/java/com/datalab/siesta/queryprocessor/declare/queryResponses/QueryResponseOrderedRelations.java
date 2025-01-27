package com.datalab.siesta.queryprocessor.declare.queryResponses;

import com.datalab.siesta.queryprocessor.declare.model.EventPairSupport;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class QueryResponseOrderedRelations implements QueryResponse {

    private String mode;
    private List<EventPairSupport> response;
    private List<EventPairSupport> precedence;
    private List<EventPairSupport> succession;
    @JsonProperty("not-succession")
    private List<EventPairSupport> notSuccession;

    public QueryResponseOrderedRelations() {
        response=new ArrayList<>();
        precedence=new ArrayList<>();
        succession=new ArrayList<>();
        notSuccession=new ArrayList<>();
    }

    public QueryResponseOrderedRelations(String mode) {
        this.mode = mode;
        response=new ArrayList<>();
        precedence=new ArrayList<>();
        succession=new ArrayList<>();
        notSuccession=new ArrayList<>();
    }

    @JsonIgnore
    public QueryResponseOrderedRelations getQueryResponseOrderedRelations() {
        QueryResponseOrderedRelations queryResponseOrderedRelations = new QueryResponseOrderedRelations();
        queryResponseOrderedRelations.setMode(this.getMode());
        queryResponseOrderedRelations.setResponse(this.getResponse());
        queryResponseOrderedRelations.setPrecedence(this.getPrecedence());
        queryResponseOrderedRelations.setSuccession(this.getSuccession());
        queryResponseOrderedRelations.setNotSuccession(this.getNotSuccession());
        return queryResponseOrderedRelations;
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
        this.response.addAll(response);
    }

    public List<EventPairSupport> getPrecedence() {
        return precedence;
    }

    public void setPrecedence(List<EventPairSupport> precedence) {
        this.precedence.addAll(precedence);
    }

    public List<EventPairSupport> getSuccession() {
        return succession;
    }

    public void setSuccession(List<EventPairSupport> succession) {
        this.succession.addAll(succession);
    }

    public List<EventPairSupport> getNotSuccession() {
        return notSuccession;
    }

    public void setNotSuccession(List<EventPairSupport> notSuccession) {
        this.notSuccession.addAll(notSuccession);
    }
}
