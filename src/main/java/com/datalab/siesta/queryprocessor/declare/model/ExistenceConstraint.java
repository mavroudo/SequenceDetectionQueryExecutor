package com.datalab.siesta.queryprocessor.declare.model;

import java.io.Serializable;

import lombok.Setter;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Setter
@NoArgsConstructor
public class ExistenceConstraint implements Serializable{

    private String rule;
    private String event_type;
    private int n;
    private double occurrences;

    public ExistenceConstraint(String rule, String event_type, int n, double occurrences){
        this.rule=rule;
        this.event_type=event_type;
        this.n=n;
        this.occurrences=occurrences;
    }

}
