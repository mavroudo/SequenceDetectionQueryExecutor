package com.datalab.siesta.queryprocessor.model.Constraints;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;

import java.io.Serializable;


public class TimeConstraint extends Constraint implements Cloneable, Serializable {
    /**
     * constraint is referring to the seconds gap between 2 events
     */
    private long constraint;

    private String granularity;

    public TimeConstraint() {
        super("within");
        granularity="seconds";
    }

    public TimeConstraint(int posA, int posB, long constraint) {
        super(posA, posB);
        this.constraint = constraint;
        granularity="seconds";
    }

    public TimeConstraint(int posA, int posB, long constraint, String granularity) {
        super(posA, posB);
        this.constraint = constraint;
        this.granularity = granularity;
    }

    public boolean isConstraintHolds(Count c) {
        if (method.equals("within")) {
            return c.getMin_duration() <= this.getConstraint();
        } else if (method.equals("atleast")) {
            return this.getConstraint() <= c.getMax_duration();
        } else return false;
    }

    public long getConstraint() {
        if(granularity.equals("minutes")) return constraint*60;
        else if (granularity.equals("hours")) return constraint*60*60;
        else return constraint;
    }

    public void setConstraint(long constraint) {
        this.constraint = constraint;
    }


    @Override
    public TimeConstraint clone() {
        TimeConstraint clone = (TimeConstraint) super.clone();
        clone.setConstraint(constraint);
        clone.setGranularity(granularity);
        return clone;
    }

    public String getGranularity() {
        return granularity;
    }

    public void setGranularity(String granularity) {
        this.granularity = granularity;
    }

    @Override
    /**
     * a should be before b
     */
    public boolean isCorrect(EventBoth a, EventBoth b) {
        if (a.getTimestamp() == null || b.getTimestamp() == null) return false;
        if (this.method.equals("within")) {
            return (b.getTimestamp().getTime() - a.getTimestamp().getTime()) / 1000 <= this.getConstraint();
        } else return (b.getTimestamp().getTime() - a.getTimestamp().getTime()) / 1000 >= this.getConstraint();
    }

    @Override
    /**
     * Returns the minimum time in seconds that will be required for event a to come within range of
     */
    public long minimumChangeRequired(EventBoth a, EventBoth b) {
        if (a.getTimestamp() == null || b.getTimestamp() == null) return -1;
        if (this.method.equals("within")) {
            return (b.getTimestamp().getTime() / 1000) - (a.getTimestamp().getTime() / 1000) - this.getConstraint();
        } else return (a.getTimestamp().getTime() / 1000) + this.getConstraint() - (b.getTimestamp().getTime() / 1000);
    }
}
