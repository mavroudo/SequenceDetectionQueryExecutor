package com.datalab.siesta.queryprocessor.model.Constraints;

public abstract class Constraint {

    //position of the first event in the Pattern
    protected int posA;
    protected int posB;

    public Constraint() {
    }

    public Constraint(int posA, int posB) {
        this.posA = posA;
        this.posB = posB;
    }

    public int getPosA() {
        return posA;
    }

    public void setPosA(int posA) {
        this.posA = posA;
    }

    public int getPosB() {
        return posB;
    }

    public void setPosB(int posB) {
        this.posB = posB;
    }
}
