package com.datalab.siesta.queryprocessor.model;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import org.codehaus.jackson.annotate.JsonIgnore;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class PossibleOrderOfEvents {

    private long trace_id;
    private List<Event> events;
    private int posEventChanged;
    private long change;
    private boolean hasChanged;
    private List<Constraint> constraints;
    private int k;

    /**
     * Constructor of the original PossiblePatterns (before changes)
     * @param trace_id
     * @param events
     * @param constraints
     */
    public PossibleOrderOfEvents(long trace_id, List<Event> events, List<Constraint> constraints, int k) {
        this.hasChanged=false;
        this.change=0;
        this.posEventChanged=-1;
        this.trace_id = trace_id;
        this.events = events;
        this.constraints = constraints;
        this.k=k;
    }

    /**
     * Constructor of the possible modifications that can take place
     * @param trace_id
     * @param events
     * @param posEventChanged
     * @param change
     * @param constraints
     * @param k
     */
    public PossibleOrderOfEvents(long trace_id, List<Event> events, int posEventChanged, long change,
                                 List<Constraint> constraints, int k, boolean hasChanged) {
        this.hasChanged=hasChanged;
        this.trace_id = trace_id;
        this.events = events;
        this.posEventChanged = posEventChanged;
        this.change = change;
        this.constraints = constraints;
        this.k = k;
    }

    @JsonIgnore
    public List<PossibleOrderOfEvents> getPossibleOrderOfEvents(){
        List<PossibleOrderOfEvents> order = new ArrayList<>();
        order.add(this);
        List<Tuple2<Integer,String >> constraintTypes = this.constraints.stream().parallel().flatMap(constraint -> {
            List<Tuple2<Integer,String>> l = new ArrayList<>();
            for(Constraint c: this.constraints){
                if(c.getMethod().equals("within")){
                    l.add(new Tuple2<>(c.getPosA(),"front")); //allowed movement
                    l.add(new Tuple2<>(c.getPosB(),"back"));
                }else{
                    l.add(new Tuple2<>(c.getPosA(),"back"));
                    l.add(new Tuple2<>(c.getPosB(),"front"));
                }
            }
            return l.stream();
        }).collect(Collectors.toList());

        for(Tuple2<Integer,String> c: constraintTypes) {
            boolean stop = false;
            if (c._2.equals("front")) {
                int e = c._1 + 1; //starting from the next event
                while (!stop && e < this.events.size()) {
                    long diff = this.events.get(c._1).calculateDiff(this.events.get(e));
                    if (diff < k) {
                        List<Event> newEvents = new ArrayList<>(){
                            {
                                for(Event event:events){
                                    this.add(event.clone());
                                }
                            }
                        };
                        long prevValue = newEvents.get(c._1).getPrimaryMetric();
                        newEvents.get(c._1).setPrimaryMetric(prevValue+diff+1);
                        Collections.swap(newEvents, c._1, e);
                        order.add(new PossibleOrderOfEvents(trace_id, newEvents, e, diff+1,constraints,k,true));
                        e += 1;
                    }else{
                        stop=true;
                    }
                }
            }else{
                int e = c._1 -1; //starting from the previous event
                while (!stop && e >= 0) {
                    long diff = this.events.get(c._1).calculateDiff(this.events.get(e));
                    if (Math.abs(diff) < k) {
                        List<Event> newEvents = new ArrayList<>(){
                            {
                                for(Event event:events){
                                    this.add(event.clone());
                                }
                            }
                        };
                        long prevValue = newEvents.get(c._1).getPrimaryMetric();
                        newEvents.get(c._1).setPrimaryMetric(prevValue+diff-1);
                        Collections.swap(newEvents, c._1, e);
                        order.add(new PossibleOrderOfEvents(trace_id, newEvents, e, diff-1,constraints,k,true));
                        e -= 1;
                    }else{
                        stop=true;
                    }
                }
            }
        }


        return order;


    }

    @JsonIgnore
    public boolean evaluatePatternConstraints(Occurrence occurrence){
        //TODO: implement this logic
        //return true if the events in the occurrence meet the constraints by changing one particular event up to k


        return false;
    }

    public long getTrace_id() {
        return trace_id;
    }

    public void setTrace_id(long trace_id) {
        this.trace_id = trace_id;
    }

    public List<Event> getEvents() {
        return events;
    }

    public void setEvents(List<Event> events) {
        this.events = events;
    }

    public int getPosEventChanged() {
        return posEventChanged;
    }

    public void setPosEventChanged(int posEventChanged) {
        this.posEventChanged = posEventChanged;
    }

    public long getChange() {
        return change;
    }

    public void setChange(long change) {
        this.change = change;
    }

    public boolean isHasChanged() {
        return hasChanged;
    }

    public void setHasChanged(boolean hasChanged) {
        this.hasChanged = hasChanged;
    }

    public List<Constraint> getConstraints() {
        return constraints;
    }

    public void setConstraints(List<Constraint> constraints) {
        this.constraints = constraints;
    }

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PossibleOrderOfEvents that = (PossibleOrderOfEvents) o;
        return trace_id == that.trace_id && events.equals(that.events);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trace_id, events);
    }
}
