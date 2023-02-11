package com.datalab.siesta.queryprocessor.model.Patterns;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraint;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraint;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import edu.umass.cs.sase.query.State;
import org.codehaus.jackson.annotate.JsonIgnore;

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
    public Set<EventPair> extractPairsForPatternDetection(){
        return  super.extractPairsForPatternDetection(this.events,this.getConstraints());

    }


    @JsonIgnore
    public Set<EventPair> extractPairsConsecutive() {
        return this.extractPairsConsecutive(this.events,this.getConsecutiveConstraints());
    }

    @JsonIgnore
    @Override
    public List<String> getEventTypes(){
        return this.events.stream().map(Event::getName).collect(Collectors.toList());
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
            this.generatePredicates(i).forEach(s::addPredicate);
            states[i] = s;
        }
        return states;
    }

    /**
     * Generate the strings that dictates the predicates. i value is equal to state number -1, and that is because
     * states starts from 1 and states[] startrs from 0
     * @param i equal to the number of state -1
     * @return a list of the preicates for this state
     */
    private List<String> generatePredicates(int i){
        List<String> response = new ArrayList<>();
        for(Constraint c: this.constraints){
            if(c.getPosB()==i && c instanceof GapConstraint){
                GapConstraint gc = (GapConstraint) c;
                if(gc.getMethod().equals("within")) response.add(String.format(" position <= $%d.position + %d ",c.getPosA()+1,gc.getConstraint()));
                else response.add(String.format(" position >= $%d.position + %d ",c.getPosA()+1,gc.getConstraint())); //atleast
            }else if(c.getPosB()==i && c instanceof TimeConstraint){
                TimeConstraint tc = (TimeConstraint) c;
                if(tc.getMethod().equals("within")) response.add(String.format(" timestamp <= $%d.timestamp + %d ",c.getPosA()+1,tc.getConstraint()));
                else response.add(String.format(" timestamp >= $%d.timestamp + %d ",c.getPosA()+1,tc.getConstraint())); //atleast
            }
        }
        return response;
    }

    @Override
    @JsonIgnore
    public int getSize() {
        return this.events.size();
    }
}
