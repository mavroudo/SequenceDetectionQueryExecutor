package com.datalab.siesta.queryprocessor.model.Events;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseEvent;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.MappingJacksonViews;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonView;

import java.io.Serializable;
import java.util.Objects;

public class Event implements Serializable, Comparable {


    protected String name;

    protected long traceID;

    public Event() {
        this.name="";
    }

    public Event(String name) {
        this.name = name;
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


}
