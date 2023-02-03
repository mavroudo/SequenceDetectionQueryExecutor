package com.datalab.siesta.queryprocessor.model.Events;

import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.MappingJacksonViews;
import com.fasterxml.jackson.annotation.JsonView;

import java.sql.Timestamp;

public class EventTs extends Event{

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


}
