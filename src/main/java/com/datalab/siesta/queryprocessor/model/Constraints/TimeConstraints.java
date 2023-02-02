package com.datalab.siesta.queryprocessor.model.Constraints;

public class TimeConstraints extends Constraint {

    private long constraint;

    public TimeConstraints() {
    }

    public TimeConstraints(int posA, int posB, long constraint) {
        super(posA, posB);
        this.constraint = constraint;

    }

    public long getConstraint() {
        return constraint;
    }

    public void setConstraint(long constraint) {
        this.constraint = constraint;
    }

    @Override
    public String toString() {
        return "TimeConstraints{" +
                "constraint=" + constraint +
                ", posA=" + posA +
                ", posB=" + posB +
                '}';
    }
}
