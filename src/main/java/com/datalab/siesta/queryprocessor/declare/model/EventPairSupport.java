package com.datalab.siesta.queryprocessor.declare.model;



import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.Serializable;
import java.util.Objects;

public class EventPairSupport implements Serializable {

    @JsonProperty("evA")
    private String eventA;
    @JsonProperty("evB")
    private String eventB;
    @JsonProperty("support")
    @JsonSerialize(using = SupportSerializer.class)
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventPairSupport that = (EventPairSupport) o;
        return Double.compare(support, that.support) == 0 && Objects.equals(eventA, that.eventA) && Objects.equals(eventB, that.eventB);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventA, eventB, support);
    }

    public void setSupport(double support) {
        this.support = support;
    }
}
