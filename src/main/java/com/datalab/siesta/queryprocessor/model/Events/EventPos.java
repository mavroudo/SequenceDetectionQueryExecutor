package com.datalab.siesta.queryprocessor.model.Events;

import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.MappingJacksonViews;
import com.fasterxml.jackson.annotation.JsonView;

public class EventPos extends Event{

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


}
