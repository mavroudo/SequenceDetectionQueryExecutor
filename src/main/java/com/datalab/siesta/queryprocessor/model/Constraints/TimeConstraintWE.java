package com.datalab.siesta.queryprocessor.model.Constraints;

import com.datalab.siesta.queryprocessor.model.DBModel.IndexPair;
import com.datalab.siesta.queryprocessor.model.EventPair;

import java.io.Serializable;

public class TimeConstraintWE extends ConstraintWithEvents implements Serializable {

    private long constraint;

    public TimeConstraintWE(String eventA, String eventB, String method, long constraint) {
        super(eventA, eventB,method);
        this.constraint = constraint;
    }

    public TimeConstraintWE() {

    }

    public TimeConstraintWE(EventPair e){
        this.eventA=e.getEventA().getName();
        this.eventB=e.getEventB().getName();
        TimeConstraint tc = (TimeConstraint) e.getConstraint();
        this.constraint=tc.getConstraint();
        this.method=tc.getMethod();
    }

    @Override
    public boolean isConstraintTrue(IndexPair e) {
        if(e.getTimestampA()==null || e.getTimestampB()==null){
            return true;
        }
        long diff = (e.getTimestampB().getTime() -e.getTimestampA().getTime())/1000; //In seconds
        if(this.method.equals("within") && diff<=this.constraint) return true;
        else return this.method.equals("atleast") && diff >= this.constraint;
    }

    public long getConstraint() {
        return constraint;
    }

    public void setConstraint(long constraint) {
        this.constraint = constraint;
    }
}
