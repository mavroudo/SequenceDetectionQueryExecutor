package com.datalab.siesta.queryprocessor.model.Constraints;

import com.datalab.siesta.queryprocessor.model.Events.EventBoth;

import java.io.Serializable;

/**
 * Constraint based on the position-distance between the two events in a event-pair
 */
public class GapConstraint extends Constraint implements Cloneable, Serializable {

    private int constraint;

    public GapConstraint() {
        super("within");
    }

    public GapConstraint(int posA, int posB, int constraint) {
        super(posA, posB);
        this.constraint = constraint;
    }

    public int getConstraint() {
        return constraint;
    }

    public void setConstraint(int constraint) {
        this.constraint = constraint;
    }


    @Override
    public GapConstraint clone() {
        GapConstraint clone = (GapConstraint) super.clone();
        clone.setConstraint(constraint);
        return clone;
    }

    @Override
    /**
     * a should be before b
     */
    public boolean isCorrect(EventBoth a, EventBoth b) {
        if (a.getPosition() == -1 || b.getPosition() == -1) return false;
        if (this.method.equals("within")) {
            return b.getPosition() - a.getPosition() <= constraint;
        }else return b.getPosition() - a.getPosition() >= constraint;
    }

    @Override
    public long minimumChangeRequired(EventBoth a, EventBoth b) {
        if (a.getPosition() == -1 || b.getPosition() == -1) return -1;
        if(this.method.equals("within")){
            return b.getPosition()-a.getPosition()-constraint;
        }else return a.getPosition()+constraint-b.getPosition();
    }
}
