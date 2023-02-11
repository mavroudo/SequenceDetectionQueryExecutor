package com.datalab.siesta.queryprocessor.model.Events;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseEvent;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.MappingJacksonViews;
import com.fasterxml.jackson.annotation.JsonView;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Objects;

public class EventTs extends Event implements Serializable {

    @JsonView(MappingJacksonViews.EventAllInfo.class)
    protected Timestamp timestamp;



    public EventTs() {
        this.timestamp=null;
    }

    public EventTs(String name, Timestamp ts) {
        super(name);
        this.timestamp=ts;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    @JsonIgnore
    public EventBoth getEventBoth(){
        return new EventBoth(this.name,this.timestamp,-1);
    }


    @JsonIgnore
    public SaseEvent transformSaseEvent(int position, long minTs) {
        SaseEvent se = super.transformSaseEvent(position);
        se.setTimestamp((int)((this.timestamp.getTime()-minTs)/1000)); //transform to differences in seconds
        se.setTimestampSet(true);
        se.setMinTs(minTs);
        return se;
    }

    @Override
    public int compareTo(Object o) {
        EventTs ep = (EventTs) o;
        return this.timestamp.compareTo(ep.getTimestamp());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        EventTs eventTs = (EventTs) o;
        return Objects.equals(timestamp, eventTs.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), timestamp);
    }
}
