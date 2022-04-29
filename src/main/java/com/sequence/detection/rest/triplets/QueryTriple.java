package com.sequence.detection.rest.triplets;

import com.sequence.detection.rest.model.Event;
import com.sequence.detection.rest.model.QueryPair;

import java.util.List;
import java.util.Objects;

public class QueryTriple extends QueryPair {
    private final Event third_ev;

    public QueryTriple(Event first_ev, Event second_ev, Event third_ev) {
        super(first_ev, second_ev);
        this.third_ev = third_ev;
    }

    public Event getThird_ev() {
        return third_ev;
    }

    @Override
    public List<Event> getEvents() {
        List<Event> events = super.getEvents();
        events.add(this.third_ev);
        return events;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        QueryTriple that = (QueryTriple) o;
        if (!Objects.equals(this.first_ev, that.first_ev))
            return false;
        if (!Objects.equals(this.second_ev, that.second_ev))
            return false;
        return Objects.equals(this.third_ev, that.third_ev);
    }

    @Override
    public int hashCode() {
        int hash = super.hashCode();
        hash=19*hash + Objects.hashCode(this.third_ev);
        return hash;
    }

    @Override
    public String toString()
    {
        return "(" + first_ev + "," + second_ev+","+this.third_ev + ")";
    }
}
