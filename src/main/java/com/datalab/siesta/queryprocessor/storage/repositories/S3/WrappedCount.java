package com.datalab.siesta.queryprocessor.storage.repositories.S3;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;

import java.util.List;

public class WrappedCount {

    private String eventA;

    private List<CountRecords> counts;

    public WrappedCount(String eventA, List<CountRecords> counts) {
        this.eventA = eventA;
        this.counts = counts;
    }

    public WrappedCount() {
    }

    public String getEventA() {
        return eventA;
    }

    public void setEventA(String eventA) {
        this.eventA = eventA;
    }

    public List<CountRecords> getCounts() {
        return counts;
    }

    public void setCounts(List<CountRecords> counts) {
        this.counts = counts;
    }
}
