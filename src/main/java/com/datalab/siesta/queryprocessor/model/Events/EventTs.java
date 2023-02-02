package com.datalab.siesta.queryprocessor.model.Events;

import java.sql.Timestamp;

public class EventTs extends Event{

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
    public String toString() {
        return "EventTs{" +
                "timestamp=" + timestamp +
                ", name='" + name + '\'' +
                "} ";
    }
}
