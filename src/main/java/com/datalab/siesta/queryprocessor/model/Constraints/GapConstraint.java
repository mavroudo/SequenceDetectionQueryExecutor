package com.datalab.siesta.queryprocessor.model.Constraints;

import java.io.Serializable;

public class GapConstraint extends Constraint implements Cloneable, Serializable {

    private int constraint;

    public GapConstraint() {
        super();
    }

    public GapConstraint(int posA, int posB, int constraint) {
        super(posA, posB);
        this.constraint=constraint;
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
}
