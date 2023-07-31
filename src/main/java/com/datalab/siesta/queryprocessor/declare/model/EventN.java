package com.datalab.siesta.queryprocessor.declare.model;

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
