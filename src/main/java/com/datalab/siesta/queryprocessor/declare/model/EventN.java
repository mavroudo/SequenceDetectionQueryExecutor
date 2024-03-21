package com.datalab.siesta.queryprocessor.declare.model;

/**
 * Extends event support by adding one more field and it is used in the absence/existence/exactly
 * templates
 */
public class EventN extends EventSupport{

    private int n;

    public EventN() {
        super();
    }

    public EventN(String event, int n, double support) {
        super(event,support);
        this.n=n;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }
}
