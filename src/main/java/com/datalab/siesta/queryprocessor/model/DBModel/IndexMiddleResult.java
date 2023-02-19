package com.datalab.siesta.queryprocessor.model.DBModel;

import com.datalab.siesta.queryprocessor.model.Events.Event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexMiddleResult {


    private List<Long> trace_ids;

    private Map<Long, List<Event>> events;


    public IndexMiddleResult() {
        this.trace_ids = new ArrayList<>();
        this.events = new HashMap<>();
    }

    public IndexMiddleResult(List<Long> trace_ids, Map<Long, List<Event>> events) {
        this.trace_ids = trace_ids;
        this.events = events;
    }

    public List<Long> getTrace_ids() {
        return trace_ids;
    }

    public void setTrace_ids(List<Long> trace_ids) {
        this.trace_ids = trace_ids;
    }

    public Map<Long, List<Event>> getEvents() {
        return events;
    }

    public void setEvents(Map<Long, List<Event>> events) {
        this.events = events;
    }
}
