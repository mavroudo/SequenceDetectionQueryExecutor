package com.datalab.siesta.queryprocessor.model;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.ArrayList;
import java.util.List;

public class PossiblePattern {

    private long trace_id;
    private List<Event> events;
    private int posEventChanged;
    private int change;
    private boolean hasChanged;
    private List<Constraint> constraints;
    private int k;

    /**
     * Constructor of the original PossiblePatterns (before changes)
     * @param trace_id
     * @param events
     * @param constraints
     */
    public PossiblePattern(long trace_id, List<Event> events, List<Constraint> constraints, int k) {
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
    public PossiblePattern(long trace_id, List<Event> events, int posEventChanged, int change,
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
    public List<PossiblePattern> getPossiblePatterns(){
        return new ArrayList<>(); //TODO: implement this logic
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

    public int getChange() {
        return change;
    }

    public void setChange(int change) {
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
}
