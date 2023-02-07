package com.datalab.siesta.queryprocessor.model.Events;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseEvent;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.MappingJacksonViews;
import com.fasterxml.jackson.annotation.JsonView;

import java.io.Serializable;
import java.sql.Timestamp;

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
    public EventBoth getEventBoth(){
        return new EventBoth(this.name,this.timestamp,-1);
    }

    @Override
    public SaseEvent transformSaseEvent(int position) {
        SaseEvent se = super.transformSaseEvent(position);
        se.setTimestamp((int)this.timestamp.getTime()/1000); //transform to seconds
        return se;
    }

}
