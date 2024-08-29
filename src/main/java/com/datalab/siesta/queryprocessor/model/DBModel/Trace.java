package com.datalab.siesta.queryprocessor.model.DBModel;

import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import lombok.Getter;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A sequence of events. It is represented by a trace id and a list of the events it contains in the correct order.
 */

public class Trace implements Serializable {

    private String traceID;

    private List<EventBoth> events;

    public Trace() {
    }

    public Trace(String traceID, List<EventBoth> events) {
        this.traceID = traceID;
        this.events = events;
    }

    public void filter(Timestamp from, Timestamp till){
        events= events.stream().filter(x-> from==null ||  !x.getTimestamp().before(from))
                .filter(x -> till==null || !x.getTimestamp().after(till))
                .collect(Collectors.toList());
    }

    public String getTraceID() {
        return traceID;
    }

    public void setTraceID(String traceID) {
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
