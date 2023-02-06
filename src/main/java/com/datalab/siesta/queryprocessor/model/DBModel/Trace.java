package com.datalab.siesta.queryprocessor.model.DBModel;

import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Trace implements Serializable {

    private long traceID;

    private List<EventBoth> events;

    public Trace() {
    }

    public Trace(long traceID, List<EventBoth> events) {
        this.traceID = traceID;
        this.events = events;
    }

    public long getTraceID() {
        return traceID;
    }

    public void setTraceID(long traceID) {
        this.traceID = traceID;
    }

    public List<EventBoth> getEvents() {
        return events;
    }

    public void setEvents(List<EventBoth> events) {
        this.events = events;
    }

    public List<EventBoth> clearTrace(Set<String> events_types){
        List<EventBoth> result = new ArrayList<>();
        for(EventBoth eb : this.events){
            if(events_types.contains(eb.getName())) result.add(eb);
        }
        return result;
    }
}
