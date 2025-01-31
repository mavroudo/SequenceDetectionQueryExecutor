package com.datalab.siesta.queryprocessor.declare.model;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

public class EventSupport {

    @JsonProperty("ev")
    protected String event;
    @JsonProperty("support")
    @JsonSerialize(using = SupportSerializer.class)
    protected Double support;

    public EventSupport(String event, double support) {
        this.event = event;
        this.support = support;
    }

    public EventSupport() {
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public double getSupport() {
        return support;
    }

    public void setSupport(double support) {
        this.support = support;
    }
}
