package com.datalab.siesta.queryprocessor.model.Constraints;

import jnr.ffi.provider.jffi.ClosureFromNativeConverter;

public abstract class Constraint implements Cloneable {

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

    @Override
    public String toString() {
        return "Constraint{" +
                "posA=" + posA +
                ", posB=" + posB +
                '}';
    }

    @Override
    public Constraint clone() {
        try {
            Constraint clone = (Constraint) super.clone();
            clone.setPosA(this.getPosA());
            clone.setPosB(this.posB);
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}
