package com.datalab.siesta.queryprocessor.declare.model.declareState;

import java.io.Serializable;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor

public class UnorderStateI implements Serializable{

    private String _1;
    private String _2;
    private long _3;

    public UnorderStateI(String _1, String _2, long _3) {
        this._1 = _1;
        this._2 = _2;
        this._3 = _3;
    }

}
