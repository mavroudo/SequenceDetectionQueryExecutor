package com.datalab.siesta.queryprocessor.model;

import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;

import java.util.List;

public class Occurrence {


    private List<EventBoth> occurrence;

    public Occurrence(List<EventBoth> occurrence) {
        this.occurrence = occurrence;
    }

    public List<EventBoth> getOccurrence() {
        return occurrence;
    }

    public void setOccurrence(List<EventBoth> occurrence) {
        this.occurrence = occurrence;
    }
}
