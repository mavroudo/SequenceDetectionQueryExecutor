package com.sequence.detection.rest.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A pair of events (along with their details) corresponding to a query pair (e.g. a 3-sized funnel has 3 query pairs)
 * @author Andreas Kosmatopoulos
 */
public class QueryPair
{
    final Event first_ev;
    final Event second_ev;

    public QueryPair(Event first_ev, Event second_ev)
    {
        this.first_ev = first_ev;
        this.second_ev = second_ev;
    }

    public Event getFirst()
    {
        return first_ev;
    }

    public Event getSecond()
    {
        return second_ev;
    }

    public List<Event> getEvents(){
        List<Event> events = new ArrayList<>();
        events.add(this.getFirst());
        events.add(this.getSecond());
        return events;
    }

    @Override
    public final int hashCode()
    {
        int hash = 5;
        hash = 19 * hash + Objects.hashCode(this.first_ev);
        hash = 19 * hash + Objects.hashCode(this.second_ev);
        return hash;
    }

    @Override
    public final boolean equals(Object obj)
    {
        if (!(obj instanceof QueryPair))
            return false;
        if (this == obj)
            return true;
        final QueryPair other = (QueryPair) obj;
        if (!Objects.equals(this.first_ev, other.first_ev))
            return false;
        
        return Objects.equals(this.second_ev, other.second_ev);
    }

    @Override
    public String toString()
    {
        return "(" + first_ev + "," + second_ev + ")";
    }
}
