package com.datalab.siesta.queryprocessor.model.Patterns;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SimplePattern {

    private List<EventPos> events;

    private List<Constraint> constraints;

    public SimplePattern() {
        events = new ArrayList<>();
        constraints = new ArrayList<>();
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
        this.fixConstraints();
    }

    @Override
    public String toString() {
        return "SimplePattern{" +
                "events=" + events +
                ", constraints=" + constraints +
                '}';
    }

    public Set<EventPair> extractPairsAll() {
        Set<EventPair> eventPairs = new HashSet<>();
        for (int i = 0; i < this.events.size() - 1; i++) {
            for (int j = i+1; j < this.events.size(); j++) {
                EventPair n = new EventPair(this.events.get(i), this.events.get(j));
                Constraint c = this.searchForConstraint(i, j);
                if (c != null) n.setConstraint(c);
                eventPairs.add(n);
            }
        }
        return eventPairs;
    }

    public Set<EventPair> extractPairsConsecutive() {
        Set<EventPair> eventPairs = new HashSet<>();
        for(int i =0 ; i< events.size()-1; i++){
            EventPair n = new EventPair(this.events.get(i), this.events.get(i+1));
            Constraint c = this.searchForConstraint(i, i+1);
            if (c != null) n.setConstraint(c);
            eventPairs.add(n);
        }
        return eventPairs;
    }

    protected Constraint searchForConstraint(int posA, int posB) {
        for (Constraint c : this.constraints) {
            if (c.getPosA() == posA && c.getPosB() == posB) {
                return c;
            }
        }
        return null;
    }

    protected void fixConstraints() {
        for (Constraint c : this.constraints) {
            for (int i = c.getPosA() + 1; i < c.getPosB(); i++) {
                Constraint c1 = c.clone();
                c1.setPosA(c.getPosA());
                c1.setPosB(i);
                this.constraints.add(c1);
            }
        }
    }
}
