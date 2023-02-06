package com.datalab.siesta.queryprocessor.model.Constraints;

import com.datalab.siesta.queryprocessor.model.DBModel.IndexPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;

import java.io.Serializable;

public class GapConstraintWE extends ConstraintWithEvents implements Serializable {
    private int constraint;

    public GapConstraintWE(String eventA, String eventB, String method, int constraint) {
        super(eventA, eventB,method);
        this.constraint = constraint;
    }

    public GapConstraintWE() {

    }

    public GapConstraintWE(EventPair e){
        this.eventA=e.getEventA().getName();
        this.eventB=e.getEventB().getName();
        GapConstraint tc = (GapConstraint) e.getConstraint();
        this.constraint=tc.getConstraint();
        this.method=tc.getMethod();
    }

    @Override
    public boolean isConstraintTrue(IndexPair e) {
        if(e.getPositionA()==-1 || e.getPositionB()==-1){
            return true;
        }
        int diff = e.getPositionB()-e.getPositionA();
        if(this.method.equals("within") && diff<=this.constraint) return true;
        else return this.method.equals("atleast") && diff >= this.constraint;
    }

    public int getConstraint() {
        return constraint;
    }

    public void setConstraint(int constraint) {
        this.constraint = constraint;
    }
}
