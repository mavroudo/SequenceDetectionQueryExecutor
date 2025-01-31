package com.datalab.siesta.queryprocessor.model.DBModel;

import com.datalab.siesta.queryprocessor.model.Events.Event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class handles the middle results in pattern detection queries. More specifically, it contains the id of
 * the traces that have occurrences of all the required et-pairs, along with the exact events (fetched from the
 * IndexTable)
 */
public class IndexMiddleResult {


    private List<String> trace_ids;

    private Map<String, List<Event>> events;


    public IndexMiddleResult() {
        this.trace_ids = new ArrayList<>();
        this.events = new HashMap<>();
    }

    public IndexMiddleResult(List<String> trace_ids, Map<String, List<Event>> events) {
        this.trace_ids = trace_ids;
        this.events = events;
    }

    public List<String> getTrace_ids() {
        return trace_ids;
    }

    public void setTrace_ids(List<String> trace_ids) {
        this.trace_ids = trace_ids;
    }

    public Map<String, List<Event>> getEvents() {
        return events;
    }

    public void setEvents(Map<String, List<Event>> events) {
        this.events = events;
    }
}
