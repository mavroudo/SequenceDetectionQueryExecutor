package com.datalab.siesta.queryprocessor.model.Constraints;

import com.datalab.siesta.queryprocessor.model.DBModel.IndexPair;
import com.datalab.siesta.queryprocessor.model.EventPair;

import java.io.Serializable;

public abstract class ConstraintWithEvents {

    protected String eventA;

    protected String eventB;

    protected String method;

    public ConstraintWithEvents(String eventA, String eventB, String method) {
        this.eventA = eventA;
        this.eventB = eventB;
        this.method=method;
    }

    public boolean isForThisConstraint(EventPair e){
        return e.getEventA().getName().equals(eventA) && e.getEventB().getName().equals(eventB);
    }

    public boolean isForThisConstraint(String eventA,String eventB){
        return eventA.equals(this.eventA) && eventB.equals(this.eventB);
    }

    public boolean isConstraintTrue(IndexPair e){
        return true;
    }

    public ConstraintWithEvents() {
    }

    public String getEventA() {
        return eventA;
    }

    public void setEventA(String eventA) {
        this.eventA = eventA;
    }

    public String getEventB() {
        return eventB;
    }

    public void setEventB(String eventB) {
        this.eventB = eventB;
    }
}
