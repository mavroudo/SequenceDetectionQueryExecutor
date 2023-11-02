package com.datalab.siesta.queryprocessor.model.WhyNotMatch.UsingSase;

import edu.umass.cs.sase.stream.Event;

/**
 * It is an implementation of the SASE Event. It contains the modified timestamp, the position of the event in the trace
 * (based on the modified timestamp) and the change that was made.
 */
public class UncertainTimeEvent implements Event, Comparable<UncertainTimeEvent> {

    private long trace_id;
    private int position;
    private String event_type;
    private int timestamp;
    private boolean isTimeStampSet;
    private int change;

    public UncertainTimeEvent() {
    }

    public UncertainTimeEvent(long trace_id, int position, String event_type, int timestamp, boolean isTimeStampSet, int change) {
        this.trace_id = trace_id;
        this.position = position;
        this.event_type = event_type;
        this.timestamp = timestamp;
        this.isTimeStampSet = isTimeStampSet;
        this.change = change;
    }

    @Override
    public int getAttributeByName(String attributeName) {
        if(attributeName.equalsIgnoreCase("position"))
            return position;
        if(attributeName.equalsIgnoreCase("timestamp"))
            return timestamp;
        if(attributeName.equalsIgnoreCase("change"))
            return change;
        return -1;
    }

    @Override
    public Object clone() {
        UncertainTimeEvent o = null;
        try {
            o = (UncertainTimeEvent)super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return o;
    }


    @Override
    public double getAttributeByNameDouble(String attributeName) {
        return 0;
    }

    @Override
    public String getAttributeByNameString(String attributeName) {
        return null;
    }

    @Override
    public int getAttributeValueType(String attributeName) {
        return 0;
    }

    @Override
    public int getId() {
        return position;
    }

    @Override
    public void setId(int Id) {
        position=Id;
    }

    @Override
    public int getTimestamp() {
        return timestamp;
    }

    @Override
    public String getEventType() {
        return event_type;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public int getChange() {
        return change;
    }

    public int getPosition() {
        return position;
    }

    /**
     * The events are ordered based on their timestamp
     * @param o the object to be compared.
     * @return the relative position between this event and o
     */
    @Override
    public int compareTo(UncertainTimeEvent o) {
        return Integer.compare(this.timestamp,o.getTimestamp());
    }
}
