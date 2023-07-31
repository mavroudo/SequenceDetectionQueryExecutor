package com.datalab.siesta.queryprocessor.declare.model;

import org.codehaus.jackson.annotate.JsonProperty;

public class EventSupport {

    @JsonProperty("ev")
    protected String event;
    @JsonProperty("support")
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
