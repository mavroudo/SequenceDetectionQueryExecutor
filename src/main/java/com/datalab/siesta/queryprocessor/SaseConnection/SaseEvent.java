package com.datalab.siesta.queryprocessor.SaseConnection;


import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import edu.umass.cs.sase.stream.Event;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.joda.time.Instant;

import java.sql.Timestamp;


/**
 * An intermediate class that is used to transforms the events of SIESTA into events that comply with the
 * SASE mechanism.
 * Since timestamps had to be integer number -> they are transformed into ms and then the minimum value is subtracted,
 * if the timestamps are not present then the position of the event in the trace is used as a timestamp
 */
public class SaseEvent implements Event {

    private String trace_id;
    private int position;

    private String eventType;
    private int timestamp;

    private boolean isTimestampSet;

    private long minTs;

    public SaseEvent() {
    }

    public long getMinTs() {
        return minTs;
    }

    public void setMinTs(long minTs) {
        this.minTs = minTs;
    }

    public SaseEvent(String trace_id, int position, String eventType, int timestamp, boolean isTimestampSet) {
        this.trace_id = trace_id;
        this.position = position;
        this.eventType = eventType;
        this.timestamp = timestamp;
        this.isTimestampSet = isTimestampSet;
        this.minTs=-1;
    }

    public boolean isTimestampSet() {
        return isTimestampSet;
    }

    public void setTimestampSet(boolean timestampSet) {
        isTimestampSet = timestampSet;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    public String getTrace_id() {
        return trace_id;
    }

    public void setTrace_id(String trace_id) {
        this.trace_id = trace_id;
    }

    @Override
    public int getAttributeByName(String attributeName) {
        if(attributeName.equalsIgnoreCase("position"))
            return position;
        if(attributeName.equalsIgnoreCase("timestamp"))
            return timestamp;
//        if(attributeName.equalsIgnoreCase("trace_id"))
//            return trace_id;
        return -1;
    }

    @JsonIgnore
    public EventBoth getEventBoth(){
        EventBoth e = new EventBoth();
        e.setName(this.eventType);
        if(isTimestampSet) {
            e.setTimestamp(new Timestamp(this.timestamp * 1000L +minTs));
        }
        if(position!=-1) {
            e.setPosition(this.position);
        }

        e.setTraceID(this.trace_id);
        return e;
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
        return this.eventType;
    }

    @Override
    public Object clone() {
        SaseEvent o = null;
        try {
            o = (SaseEvent)super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return o;
    }
}
