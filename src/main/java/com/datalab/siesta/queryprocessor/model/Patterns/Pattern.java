package com.datalab.siesta.queryprocessor.model.Patterns;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Events.Event;

import java.util.List;

public abstract class Pattern {

    protected List<Event> events;

    protected List<Constraint> constraints;

    public Pattern() {
    }

    public Pattern(List<Event> events, List<Constraint> constraints) {
        this.events = events;
        this.constraints = constraints;
    }

    public List<Event> getEvents() {
        return events;
    }

    public void setEvents(List<Event> events) {
        this.events = events;
    }

    public List<Constraint> getConstraints() {
        return constraints;
    }

    public void setConstraints(List<Constraint> constraints) {
        this.constraints = constraints;
    }

}
