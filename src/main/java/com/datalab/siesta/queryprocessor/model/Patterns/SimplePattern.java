package com.datalab.siesta.queryprocessor.model.Patterns;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import edu.umass.cs.sase.query.State;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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

    public List<String> getEventTypes(){
        return this.events.stream().map(Event::getName).collect(Collectors.toList());
    }

    public State[] getNfa(){
        State[] states = new State[this.events.size()];
        for(int i = 0; i<this.events.size();i++){
            states[i] = new State(i+1,"a",String.format("%s",this.events.get(i).getName()),"normal");
        }
        return states;
    }




}
