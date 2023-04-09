package com.datalab.siesta.queryprocessor.storage.repositories.CassandraRdd;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;

public class IndexRow implements Serializable {

    private String event_a;
    private String event_b;
    private Timestamp start;
    private Timestamp end;
    private List<String> occurrences;

    public IndexRow() {
    }

    public IndexRow(String event_a, String event_b, Timestamp start, Timestamp end, List<String> occurrences) {
        this.event_a = event_a;
        this.event_b = event_b;
        this.start = start;
        this.end = end;
        this.occurrences = occurrences;
    }

    public String getEvent_a() {
        return event_a;
    }

    public void setEvent_a(String event_a) {
        this.event_a = event_a;
    }

    public String getEvent_b() {
        return event_b;
    }

    public void setEvent_b(String event_b) {
        this.event_b = event_b;
    }

    public Timestamp getStart() {
        return start;
    }

    public void setStart(Timestamp start) {
        this.start = start;
    }

    public Timestamp getEnd() {
        return end;
    }

    public void setEnd(Timestamp end) {
        this.end = end;
    }

    public List<String> getOccurrences() {
        return occurrences;
    }

    public void setOccurrences(List<String> occurrences) {
        this.occurrences = occurrences;
    }
}
