package com.datalab.siesta.queryprocessor.model.Events;

import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.MappingJacksonViews;
import com.fasterxml.jackson.annotation.JsonView;

import java.sql.Timestamp;

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


}
