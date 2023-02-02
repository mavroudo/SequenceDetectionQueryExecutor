package com.datalab.siesta.queryprocessor.model.Constraints;

public class GapConstraint extends Constraint{

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
    public String toString() {
        return "GapConstraint{" +
                "constraint=" + constraint +
                ", posA=" + posA +
                ", posB=" + posB +
                '}';
    }
}
