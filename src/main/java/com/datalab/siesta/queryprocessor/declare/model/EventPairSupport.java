package com.datalab.siesta.queryprocessor.declare.model;

import org.codehaus.jackson.annotate.JsonProperty;

public class EventPairSupport {

    @JsonProperty("evA")
    private String eventA;
    @JsonProperty("evB")
    private String eventB;
    @JsonProperty("support")
    private double support;

    public EventPairSupport() {
    }

    public EventPairSupport(String eventA, String eventB, double support) {
        this.eventA = eventA;
        this.eventB = eventB;
        this.support = support;
    }

    public String getEventA() {
        return eventA;
    }

    public void setEventA(String eventA) {
        this.eventA = eventA;
    }

    public String getEventB() {
        return eventB;
    }

    public void setEventB(String eventB) {
        this.eventB = eventB;
    }

    public double getSupport() {
        return support;
    }

    public void setSupport(double support) {
        this.support = support;
    }
}
