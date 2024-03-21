package com.datalab.siesta.queryprocessor.declare.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class Abstract2OrderConstraint implements Serializable {
    private String eventA;
    private String eventB;
    // can be either 'r' (for response) or 'p' (for precedence)
    private String mode;
    //number that this event pair in this mode occurred (mode specifies which came first)
    private int occurrences;
}
