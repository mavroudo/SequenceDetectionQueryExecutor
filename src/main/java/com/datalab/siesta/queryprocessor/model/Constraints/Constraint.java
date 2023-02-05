package com.datalab.siesta.queryprocessor.model.Constraints;


import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

public abstract class Constraint implements Cloneable {

    //position of the first event in the Pattern
    @JsonIgnore
    protected int posA;
    @JsonIgnore
    protected int posB;

    @JsonProperty("type")
    protected String method; //It will be set to atleast or within

    public Constraint() {
        method="within";
    }

    public Constraint(int posA, int posB) {
        this.posA = posA;
        this.posB = posB;
        method="within";
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

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
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
