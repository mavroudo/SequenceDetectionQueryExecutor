package com.datalab.siesta.queryprocessor.model.Constraints;

import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.fasterxml.jackson.annotation.*;

import java.io.Serializable;

/**
 * General constraint type. It allows different type of constraints to be added in query patterns.
 * The type of constraint it is added as a json attribute (namely constraint_type). As the query wrapper
 * unravels the json in the Controller, it assigns the correct subclass of constraint based on the above
 * attribute
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "constraint_type"
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = TimeConstraint.class, name = "timeConstraint"),
        @JsonSubTypes.Type(value = GapConstraint.class, name = "gapConstraint")
})
public abstract class Constraint implements Cloneable, Serializable {

    //position of the first event in the Pattern
//    @JsonIgnore
    protected int posA;
//    @JsonIgnore
    protected int posB;

    @JsonProperty("type")
    protected String method; //It will be set to atleast or within

    public Constraint(String method){
        this.method=method;
    }

    public Constraint(int posA, int posB) {
        this.posA = posA;
        this.posB = posB;
        method="within";
    }

    @JsonCreator
    public static Constraint create(
            @JsonProperty("type") String method,
            @JsonProperty("posA") int posA,
            @JsonProperty("posB") int posB,
            @JsonProperty("constraint_type") String constraintType,
            @JsonProperty("constraint") long constraint) {
        if ("timeConstraint".equals(constraintType)) {
            return new TimeConstraint(posA, posB,constraint);
        } else if ("gapConstraint".equals(constraintType)) {
            return new GapConstraint(posA, posB, (int) constraint);
        } else {
            throw new IllegalArgumentException("Unknown constraint type: " + method);
        }
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

    @JsonIgnore
    public boolean hasError(){
        return posA>=posB;
    }

    public boolean isCorrect(EventBoth a, EventBoth b){
        return false;
    }


    public long minimumChangeRequired(EventBoth a, EventBoth b){
        return 0;
    }
}
