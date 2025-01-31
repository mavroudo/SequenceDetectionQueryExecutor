package com.datalab.siesta.queryprocessor.declare.model.declareState;

import java.io.Serializable;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class OrderState implements Serializable{

    private String rule;
    private String eventA;
    private String eventB;
    private double  occurrences;

    public OrderState(String rule, String eventA, String eventB, double occurrences) {
        this.rule = rule;
        this.eventA = eventA;
        this.eventB = eventB;
        this.occurrences = occurrences;
    }
}
