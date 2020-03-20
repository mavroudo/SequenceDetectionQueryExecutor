package com.fa.funnel.rest.model;

import java.util.Objects;

/**
 *
 * @author Andreas Kosmatopoulos
 */
public class EventPair
{
    final String first_ev;
    final String second_ev;

    public EventPair(String first_ev, String second_ev)
    {
        this.first_ev = first_ev;
        this.second_ev = second_ev;
    }

    public String getFirst()
    {
        return first_ev;
    }

    public String getSecond()
    {
        return second_ev;
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
        if (!(obj instanceof EventPair))
            return false;
        if (this == obj)
            return true;
        final EventPair other = (EventPair) obj;
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
