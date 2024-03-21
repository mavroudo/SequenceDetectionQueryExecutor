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
public class Abstract2DeclareConstraint implements Serializable {

    private String eventA;
    private String eventB;
    private long completions;
    private double support;
}
