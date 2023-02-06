package com.datalab.siesta.queryprocessor.model;

import com.datalab.siesta.queryprocessor.model.Events.Event;

import java.util.List;

public class Occurrence {

    private long traceID;

    private List<Event> occurrence;

    public Occurrence(long traceID, List<Event> occurrence) {
        this.traceID = traceID;
        this.occurrence = occurrence;
    }

    public long getTraceID() {
        return traceID;
    }

    public void setTraceID(long traceID) {
        this.traceID = traceID;
    }

    public List<Event> getOccurrence() {
        return occurrence;
    }

    public void setOccurrence(List<Event> occurrence) {
        this.occurrence = occurrence;
    }
}
