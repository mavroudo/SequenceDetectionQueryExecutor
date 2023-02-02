package com.datalab.siesta.queryprocessor.model;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Events.Event;

import java.util.Objects;

public class EventPair {

    private Event eventA;

    private Event eventB;

    private Constraint constraint;

    public Event getEventA() {
        return eventA;
    }

    public EventPair(Event eventA, Event eventB, Constraint constraint) {
        this.eventA = eventA;
        this.eventB = eventB;
        this.constraint = constraint;
    }

    public EventPair(Event eventA, Event eventB) {
        this.eventA = eventA;
        this.eventB = eventB;
        this.constraint=null;
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

    public Constraint getConstraint() {
        return constraint;
    }

    public void setConstraint(Constraint constraint) {
        this.constraint = constraint;
    }

    @Override
    public String toString() {
        return "EventPair{" +
                "eventA=" + eventA +
                ", eventB=" + eventB +
                ", constraint=" + constraint +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventPair eventPair = (EventPair) o;
        return eventA.equals(eventPair.eventA) && eventB.equals(eventPair.eventB);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventA, eventB);
    }
}
