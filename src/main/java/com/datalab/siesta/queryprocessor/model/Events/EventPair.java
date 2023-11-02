package com.datalab.siesta.queryprocessor.model.Events;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;
import java.util.Objects;

/**
 * The class that represents a et-pair
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EventPair implements Serializable {

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


    /**
     * Since this is an et-pair we are only interested in the name of the events and not in their positions
     * or timestamps
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null ) return false;
        if(o instanceof Count){
            Count k = (Count) o;
            return k.getEventA().equals(this.eventA.getName()) && k.getEventB().equals(this.eventB.getName());
        }
        if (getClass() != o.getClass()) return false;
        EventPair eventPair = (EventPair) o;
        return eventA.getName().equals(eventPair.eventA.getName()) && eventB.getName().equals(eventPair.eventB.getName());
    }


    @Override
    public int hashCode() {
        return Objects.hash(eventA.getName(), eventB.getName());
    }
}
