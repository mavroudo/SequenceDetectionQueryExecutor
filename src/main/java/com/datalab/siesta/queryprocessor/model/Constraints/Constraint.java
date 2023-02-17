package com.datalab.siesta.queryprocessor.model.Constraints;

import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import java.io.Serializable;

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
