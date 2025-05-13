package com.datalab.siesta.queryprocessor.declare.model.declareState;

import java.io.Serializable;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class UnorderStateU implements Serializable{

    private String _1;
    private long _2;

    public UnorderStateU(String _1, long _2) {
        this._1 = _1;
        this._2 = _2;
    }

}
