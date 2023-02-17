package com.datalab.siesta.queryprocessor.model.WhyNotMatch.UsingSase;

import edu.umass.cs.sase.stream.Event;

public class UncertainTimeEvent implements Event {

    private int trace_id;
    private int position;
    private String event_type;
    private int timestamp;
    private boolean isTimeStampSet;
    private int change;

    public UncertainTimeEvent() {
    }

    public UncertainTimeEvent(int trace_id, int position, String event_type, int timestamp, boolean isTimeStampSet, int change) {
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
        if(attributeName.equalsIgnoreCase("trace_id"))
            return trace_id;
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
            // TODO Auto-generated catch block
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


}
