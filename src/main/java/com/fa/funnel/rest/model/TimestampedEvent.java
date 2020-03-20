package com.fa.funnel.rest.model;

import java.util.Date;
import java.util.Objects;

/**
 * An event annotated with a {@link java.util.Date} timestamp
 * @author Andreas Kosmatopoulos
 */
public class TimestampedEvent implements Comparable<TimestampedEvent>
{
    public Date timestamp;
    public Event event;
    
    public TimestampedEvent(Date timestamp, Event event) 
    {
        this.timestamp = new Date(timestamp.getTime());
        this.event = event;
    }

    @Override
    public int compareTo(TimestampedEvent t) 
    {
        int datediff = this.timestamp.compareTo(t.timestamp);
        
        if (datediff != 0)
            return datediff;
        
        return this.event.compareTo(t.event);
    }

    @Override
    public int hashCode() 
    {
        int hash = 7;
        hash = 97 * hash + Objects.hashCode(this.timestamp);
        return hash;
    }

    @Override
    public boolean equals(Object obj) 
    {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TimestampedEvent other = (TimestampedEvent) obj;
        return Objects.equals(this.timestamp, other.timestamp);
    }
    
    @Override
    public String toString()
    {
        return "(" + event + "," + timestamp + ")";
    }
}
