package com.fa.funnel.rest.model;

import java.util.Objects;
import java.util.TreeSet;

/**
 * Represents a funnel Event
 * @author Andreas Kosmatopoulos
 */
public class Event implements Comparable<Event>
{
    /**
     * Funnel event name (prefixed by AppID and LogtypeID, i.e. appID_logtypeID_eventName)
     */
    private String name;
    /**
     * The event's accompanying details
     */
    private TreeSet<AugmentedDetail> details;
    
    /**
     * An empty event where both name and details are set to null
     */
    public static final Event EMPTY_EVENT = new Event();

    private Event()
    {
        this.name = null;
        this.details = null;
    }
    
    public Event(String name) 
    {
        this.name = name;
        this.details = new TreeSet<AugmentedDetail>();
    }
    
    public Event(String name, TreeSet<AugmentedDetail> details) 
    {
        this.name = name;
        this.details = details;
    }    
    
    public String getName() 
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public TreeSet<AugmentedDetail> getDetails() 
    {
        return details;
    }

    public void setDetails(TreeSet<AugmentedDetail> details) 
    {
        this.details = details;
    }

    public void addDetail(AugmentedDetail detail) 
    {
        details.add(detail);
    }

    @Override
    public int hashCode() 
    {
        int hash = 7;
        hash = 79 * hash + Objects.hashCode(this.name);
        hash = 79 * hash + Objects.hashCode(this.details);
        return hash;
    }

    @Override
    public boolean equals(Object obj) 
    {
        if (this == obj) 
        {
            return true;
        }
        if (obj == null) 
        {
            return false;
        }
        if (getClass() != obj.getClass()) 
        {
            return false;
        }
        final Event other = (Event) obj;
        if (!Objects.equals(this.name, other.name)) 
        {
            return false;
        }
        return Objects.equals(this.details, other.details);
    }
    
    public boolean matches(Event other)
    {
        if (!this.name.equals(other.name))
            return false;
        
        return other.details.containsAll(this.details);
    }

    @Override
    public int compareTo(Event event) 
    {
        return this.name.compareTo(event.name);
    }
    
    @Override
    public String toString()
    {
        return this.name;
    }
    
    public String getPrefixedAppID()
    {
        return this.name.split("_")[0];
    }
}
