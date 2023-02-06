package com.datalab.siesta.queryprocessor.model.Constraints;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;

import java.io.Serializable;

public class TimeConstraint extends Constraint implements Cloneable, Serializable {
    private long constraint;

    public TimeConstraint() {
        super();
    }

    public TimeConstraint(int posA, int posB, long constraint) {
        super(posA, posB);
        this.constraint = constraint;
    }

    public boolean isConstraintHolds(Count c){
        if(method.equals("within")){
            return c.getMin_duration() <= this.constraint;
        }else if (method.equals("atleast")){
            return this.constraint <= c.getMax_duration();
        }
        else return false;
    }

    public long getConstraint() {
        return constraint;
    }

    public void setConstraint(long constraint) {
        this.constraint = constraint;
    }


    @Override
    public TimeConstraint clone() {
        TimeConstraint clone = (TimeConstraint) super.clone();
        clone.setConstraint(constraint);
        return clone;
    }
}
