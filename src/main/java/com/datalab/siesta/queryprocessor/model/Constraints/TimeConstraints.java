package com.datalab.siesta.queryprocessor.model.Constraints;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;

public class TimeConstraints extends Constraint implements Cloneable{

    private long constraint;

    public TimeConstraints() {
    }

    public TimeConstraints(int posA, int posB, long constraint) {
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
    public TimeConstraints clone() {
        TimeConstraints clone = (TimeConstraints) super.clone();
        clone.setConstraint(constraint);
        return clone;
    }
}
