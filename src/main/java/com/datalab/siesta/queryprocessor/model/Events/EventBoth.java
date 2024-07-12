package com.datalab.siesta.queryprocessor.model.Events;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseEvent;
import com.datalab.siesta.queryprocessor.model.Serializations.EventBothSerializer;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.MappingJacksonViews;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;


import java.sql.Timestamp;
import java.util.Objects;

/**
 * A SIESTA event that contains both time and position information
 */
@JsonSerialize(using = EventBothSerializer.class)
public class EventBoth extends EventTs implements Comparable{

    @JsonView(MappingJacksonViews.EventAllInfo.class)
    private int position;

    public EventBoth() {
        this.position=-1;
    }

    public EventBoth(String name, Timestamp ts, int pos) {
        super(name, ts);
        this.position=pos;
    }

    public EventBoth(String name, long traceID, Timestamp timestamp, int position) {
        super(name, traceID, timestamp);
        this.position = position;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    @Override
    @JsonIgnore
    public SaseEvent transformSaseEvent(int position) {
        SaseEvent se = super.transformSaseEvent(position);
        se.setTimestamp((int)this.timestamp.getTime()/1000); //transform to seconds
        return se;
    }

    @Override
    public int compareTo(Object o) {
        if(o instanceof EventBoth){
            return this.timestamp.compareTo(((EventBoth)o).getTimestamp());
        }else if(o instanceof EventTs){
            return this.timestamp.compareTo(((EventTs)o).getTimestamp());
        } else if( o instanceof EventPos){
            return Integer.compare(this.position,((EventPos)o).getPosition());
        }
        return -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        EventBoth eventBoth = (EventBoth) o;
        return position == eventBoth.position;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), position);
    }

    @Override
    public long calculateDiff(Event e) { //return the diff in seconds
        return (((EventTs)e).getTimestamp().getTime()-this.timestamp.getTime())/1000;
    }

    @Override
    @JsonIgnore
    public long getPrimaryMetric() {
        return this.timestamp.getTime()/1000;
    }

    @Override
    public void setPrimaryMetric(long newPrimaryMetric) {
        this.timestamp= new Timestamp(newPrimaryMetric);
    }
}
