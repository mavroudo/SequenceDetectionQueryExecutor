package com.datalab.siesta.queryprocessor.model.Events;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseEvent;
import com.datalab.siesta.queryprocessor.model.Events.Serializations.EventBothSerializer;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.MappingJacksonViews;
import com.fasterxml.jackson.annotation.JsonView;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.sql.Timestamp;
import java.util.Objects;

@JsonSerialize(using = EventBothSerializer.class)
public class EventBoth extends EventTs{

    @JsonView(MappingJacksonViews.EventAllInfo.class)
    private int position;

    public EventBoth() {
        this.position=-1;
    }

    public EventBoth(String name, Timestamp ts, int pos) {
        super(name, ts);
        this.position=pos;
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
        return super.compareTo(o);
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
    public long calculateDiff(Event e) {
        return (this.timestamp.getTime()-((EventBoth)e).getTimestamp().getTime())/1000;
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
