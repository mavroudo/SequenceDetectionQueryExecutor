package com.datalab.siesta.queryprocessor.model.Patterns;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.ExtractedPairsForPatternDetection;
import edu.umass.cs.sase.query.State;
import com.fasterxml.jackson.annotation.JsonIgnore;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SimplePattern extends SIESTAPattern implements Cloneable{

    private List<EventPos> events;

    private List<Constraint> constraints;

    public SimplePattern() {
        events = new ArrayList<>();
        constraints = new ArrayList<>();
    }

    public SimplePattern(List<EventPos> events) {
        this.events = events;
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
    }

    @Override
    public String toString() {
        return "SimplePattern{" +
                "events=" + events +
                ", constraints=" + constraints +
                '}';
    }

    public List<Constraint> getConsecutiveConstraints(){ return this.constraints;}


    @JsonIgnore
    public ExtractedPairsForPatternDetection extractPairsForPatternDetection(boolean fromOrTillSet){
        return  super.extractPairsForPatternDetection(this.events,this.getConstraints(),fromOrTillSet);

    }


    @JsonIgnore
    public Set<EventPair> extractPairsConsecutive() {
        return this.extractPairsConsecutive(this.events,this.getConsecutiveConstraints());
    }

    @JsonIgnore
    @Override
    public Set<String> getEventTypes(){
        return this.events.stream().map(Event::getName).collect(Collectors.toSet());
    }

    @JsonIgnore
    @Override
    public State[] getNfaWithoutConstraints() {
        State[] states = new State[this.events.size()];
        for(int i = 0; i<this.events.size();i++){
            State s = new State(i+1,"a",String.format("%s",this.events.get(i).getName()),"normal");
            states[i] = s;
        }
        return states;
    }


    @JsonIgnore
    @Override
    public State[] getNfa(){
        State[] states = new State[this.events.size()];
        for(int i = 0; i<this.events.size();i++){
            State s = new State(i+1,"a",String.format("%s",this.events.get(i).getName()),"normal");
            this.generatePredicates(i, this.constraints).forEach(s::addPredicate);
            states[i] = s;
        }
        return states;
    }



    @Override
    @JsonIgnore
    public int getSize() {
        return this.events.size();
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        Object clone = super.clone();
        SimplePattern s = new SimplePattern();
        List<EventPos> ep = new ArrayList<>();
        for(EventPos e : this.events){
            ep.add(e.clone());
        }
        s.setEvents(ep);
        List<Constraint> cs = new ArrayList<>();
        for(Constraint c: this.constraints){
            cs.add(c.clone());
        }
        s.setConstraints(cs);
        return s;
    }
}
