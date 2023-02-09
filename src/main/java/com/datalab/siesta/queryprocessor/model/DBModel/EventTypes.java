package com.datalab.siesta.queryprocessor.model.DBModel;

import java.util.Objects;

public class EventTypes{
    private String eventA;

    private String eventB;

    public EventTypes(String eventA, String eventB) {
        this.eventA = eventA;
        this.eventB = eventB;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventTypes that = (EventTypes) o;
        return eventA.equals(that.eventA) && eventB.equals(that.eventB);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventA, eventB);
    }
}
