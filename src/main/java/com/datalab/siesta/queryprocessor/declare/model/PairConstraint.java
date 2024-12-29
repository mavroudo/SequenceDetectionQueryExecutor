package com.datalab.siesta.queryprocessor.declare.model;

import java.io.Serializable;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@NoArgsConstructor
public class PairConstraint implements Serializable{

    private EventPairSupport eventPairSupport;
    private String rule;

    public PairConstraint(EventPairSupport eventPairSupport, String rule) {
        this.eventPairSupport = eventPairSupport;
        this.rule = rule;
    }

}
