package com.datalab.siesta.queryprocessor.model.Events;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseEvent;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.MappingJacksonViews;
import com.fasterxml.jackson.annotation.JsonView;

import java.io.Serializable;
import java.util.Objects;

public class EventPos extends Event implements Serializable, Comparable {

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
    public SaseEvent transformSaseEvent(int position) {
        SaseEvent se = super.transformSaseEvent(position);
        se.setPosition(this.position);
        return se;
    }

    @Override
    public int compareTo(Object o) {
        EventPos ep = (EventPos) o;
        return Integer.compare(this.position,ep.position);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        EventPos eventPos = (EventPos) o;
        return position == eventPos.position;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), position);
    }
}
