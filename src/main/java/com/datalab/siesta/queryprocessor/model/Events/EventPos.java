package com.datalab.siesta.queryprocessor.model.Events;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseEvent;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.MappingJacksonViews;
import com.fasterxml.jackson.annotation.JsonView;

import java.io.Serializable;

public class EventPos extends Event implements Serializable {

    @JsonView(MappingJacksonViews.EventAllInfo.class)
    protected int position;


    public EventPos() {
        this.position=-1;
    }

    public EventPos(String name, int pos) {
        super(name);
        this.position=pos;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    @Override
    public EventBoth getEventBoth(){
        return new EventBoth(this.name,null,this.position);
    }

    @Override
    public SaseEvent transformSaseEvent() {
        SaseEvent se = super.transformSaseEvent();
        se.setPosition(this.position);
        return se;
    }
}
