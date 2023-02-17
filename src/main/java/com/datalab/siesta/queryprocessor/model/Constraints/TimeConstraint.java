package com.datalab.siesta.queryprocessor.model.Constraints;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;

import java.io.Serializable;


public class TimeConstraint extends Constraint implements Cloneable, Serializable {
    /**
     * constraint is referring to the seconds gap between 2 events
     */
    private long constraint;

    public TimeConstraint() {
        super();
    }

    public TimeConstraint(int posA, int posB, long constraint) {
        super(posA, posB);
        this.constraint = constraint;
    }

    public boolean isConstraintHolds(Count c) {
        if (method.equals("within")) {
            return c.getMin_duration() <= this.constraint;
        } else if (method.equals("atleast")) {
            return this.constraint <= c.getMax_duration();
        } else return false;
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

    @Override
    /**
     * a should be before b
     */
    public boolean isCorrect(EventBoth a, EventBoth b) {
        if (a.getTimestamp() == null || b.getTimestamp() == null) return false;
        if (this.method.equals("within")) {
            return (b.getTimestamp().getTime() - a.getTimestamp().getTime()) / 1000 <= constraint;
        } else return (b.getTimestamp().getTime() - a.getTimestamp().getTime()) / 1000 >= constraint;
    }

    @Override
    /**
     * Returns the minimum time in seconds that will be required for event a to come within range of
     */
    public long minimumChangeRequired(EventBoth a, EventBoth b) {
        if (a.getTimestamp() == null || b.getTimestamp() == null) return -1;
        if (this.method.equals("within")) {
            return (b.getTimestamp().getTime() / 1000) - (a.getTimestamp().getTime() / 1000) - constraint;
        } else return (a.getTimestamp().getTime() / 1000) + constraint - (b.getTimestamp().getTime() / 1000);
    }
}
