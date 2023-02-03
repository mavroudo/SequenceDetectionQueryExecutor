package com.datalab.siesta.queryprocessor.model.Patterns;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SimplePattern extends SIESTAPattern{

    private List<EventPos> events;

    private List<Constraint> constraints;

    public SimplePattern() {
        events = new ArrayList<>();
        constraints = new ArrayList<>();
    }

    public SimplePattern(List<EventPos> events) {
        this.events = events;
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
        this.fixConstraints(this.constraints);
    }

    @Override
    public String toString() {
        return "SimplePattern{" +
                "events=" + events +
                ", constraints=" + constraints +
                '}';
    }



    public Set<EventPair> extractPairsAll() {
        return this.extractPairsAll(this.events,this.constraints);
    }



    public Set<EventPair> extractPairsConsecutive() {
        return this.extractPairsConsecutive(this.events,this.constraints);
    }




}
