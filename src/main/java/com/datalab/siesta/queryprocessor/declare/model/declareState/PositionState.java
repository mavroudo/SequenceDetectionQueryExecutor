package com.datalab.siesta.queryprocessor.declare.model.declareState;

import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class PositionState implements Serializable{

    private String rule;
    private String event_type;
    private double occurrences;

    public PositionState(){
        
    }
}
