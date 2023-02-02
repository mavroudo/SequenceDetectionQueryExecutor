package com.datalab.siesta.queryprocessor.model.Events;

import java.sql.Timestamp;

public class EventBoth extends EventTs{

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
    public String toString() {
        return "EventBoth{" +
                "position=" + position +
                ", timestamp=" + timestamp +
                ", name='" + name + '\'' +
                '}';
    }
}
