package com.datalab.siesta.queryprocessor.model.Constraints;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;

public class GapConstraint extends Constraint implements Cloneable{

    private int constraint;

    public GapConstraint() {
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
