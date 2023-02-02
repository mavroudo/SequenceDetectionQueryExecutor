package com.datalab.siesta.queryprocessor.model.Patterns;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;

import java.util.ArrayList;
import java.util.List;

public class SimplePattern {

    private List<EventPos> events;

    private List<Constraint> constraints;

    public SimplePattern() {
        events=new ArrayList<>();
        constraints=new ArrayList<>();
    }

    public List<EventPos> getEvents() {
        return events;
    }

    public void setEvents(List<EventPos> events) {
        this.events = events;
    }

    public List<Constraint> getConstraints() {
        return constraints;
    }

    public void setConstraints(List<Constraint> constraints) {
        this.constraints = constraints;
    }

    @Override
    public String toString() {
        return "SimplePattern{" +
                "events=" + events +
                ", constraints=" + constraints +
                '}';
    }
}
