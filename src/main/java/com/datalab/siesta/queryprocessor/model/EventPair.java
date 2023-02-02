package com.datalab.siesta.queryprocessor.model;

import com.datalab.siesta.queryprocessor.model.Events.Event;

public class EventPair {

    private Event eventA;

    private Event eventB;

    private long constraint;

    private String type;

    public Event getEventA() {
        return eventA;
    }

    public EventPair(Event eventA, Event eventB, long constraint, String type) {
        this.eventA = eventA;
        this.eventB = eventB;
        this.constraint = constraint;
        this.type = type;
    }

    public void setEventA(Event eventA) {
        this.eventA = eventA;
    }

    public Event getEventB() {
        return eventB;
    }

    public void setEventB(Event eventB) {
        this.eventB = eventB;
    }

    public long getConstraint() {
        return constraint;
    }

    public void setConstraint(long constraint) {
        this.constraint = constraint;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "EventPair{" +
                "eventA=" + eventA +
                ", eventB=" + eventB +
                ", constraint=" + constraint +
                ", type='" + type + '\'' +
                '}';
    }
}
