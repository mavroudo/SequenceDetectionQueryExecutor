package com.datalab.siesta.queryprocessor.model.Events;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseEvent;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.MappingJacksonViews;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonView;

import java.io.Serializable;
import java.util.Objects;

/**
 * Simple event in siesta contains only the name of the activity that it is an instance of and the trace id
 * that it belongs to.
 */
public class Event implements Serializable, Comparable, Cloneable {


    protected String name;

    protected long traceID;

    public Event() {
        this.name="";
    }

    public Event(String name) {
        this.name = name;
    }

    public Event(String name, long traceID) {
        this.name = name;
        this.traceID = traceID;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Event{" +
                "name='" + name + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return name.equals(event.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    public long getTraceID() {
        return traceID;
    }

    public void setTraceID(long traceID) {
        this.traceID = traceID;
    }

    @JsonIgnore
    public EventBoth getEventBoth(){
        return new EventBoth(this.name,null,-1);
    }

    /**
     * This function transforms the current event in a SASE event in order to be processed
     * by the SASE engine
     * @param position the position of the event in the trace
     * @return a SASE event
     */
    @JsonIgnore
    public SaseEvent transformSaseEvent(int position){
        SaseEvent se= new SaseEvent();
        se.setEventType(this.name);
        se.setId(position);
        se.setTrace_id((int)this.traceID);
        se.setTimestamp(position);
        se.setTimestampSet(false);
        return se;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @JsonIgnore
    public long calculateDiff(Event e){
        return 0;
    }

    @JsonIgnore
    public long getPrimaryMetric(){
        return 0;
    }

    @JsonIgnore
    public void setPrimaryMetric(long newPrimaryMetric){
    }

    @Override
    public Event clone() {
        Event cloned = null;
        try{
            cloned=(Event) super.clone();
        }catch (CloneNotSupportedException e){
            e.printStackTrace();
        }
        return cloned;
    }


}
