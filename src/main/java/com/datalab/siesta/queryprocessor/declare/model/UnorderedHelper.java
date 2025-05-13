package com.datalab.siesta.queryprocessor.declare.model;

import java.io.Serializable;

import lombok.NoArgsConstructor;

@lombok.Getter
@lombok.Setter
@NoArgsConstructor
public class UnorderedHelper implements Serializable{
    private String eventA;
    private String eventB;
    private long ua;
    private long ub;
    private long pairs;
    private String key;

    public UnorderedHelper(String eventA, String eventB, long ua, long ub, long pairs, String key) {
        this.eventA = eventA;
        this.eventB = eventB;
        this.ua = ua;
        this.ub = ub;
        this.pairs = pairs;
        this.key = key;
    }
}
