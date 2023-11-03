package com.datalab.siesta.queryprocessor.SaseConnection;

import edu.umass.cs.sase.query.State;

/**
 * This class contains the information required to generate the NFA in SASE. An object of this class is
 * passed from the pattern to the SASE connector in order to create the State machine
 */
public class NFAWrapper {

    private String selectionStrategy;

    private int timeWindow;

    private String partitionAttribute;

    private State[] states;

    private int size;

    public NFAWrapper(String selectionStrategy) {
        this.selectionStrategy=selectionStrategy;
        this.timeWindow=Integer.MAX_VALUE;
        this.partitionAttribute="";
    }

    public String getSelectionStrategy() {
        return selectionStrategy;
    }

    public int getTimeWindow() {
        return timeWindow;
    }

    public String getPartitionAttribute() {
        return partitionAttribute;
    }

    public State[] getStates() {
        return states;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public void setSelectionStrategy(String selectionStrategy) {
        this.selectionStrategy = selectionStrategy;
    }

    public void setTimeWindow(int timeWindow) {
        this.timeWindow = timeWindow;
    }

    public void setPartitionAttribute(String partitionAttribute) {
        this.partitionAttribute = partitionAttribute;
    }

    public void setStates(State[] states) {
        this.states = states;
    }
}
