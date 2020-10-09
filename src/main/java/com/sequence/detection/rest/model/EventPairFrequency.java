package com.sequence.detection.rest.model;

/**
 * Represents an EventPairFrequency returned by probing the "count" index tables
 * @author Andreas Kosmatopoulos
 */
public class EventPairFrequency implements Comparable<EventPairFrequency>
{
    /**
     * The pair of events
     */
    EventPair ev_pair;
    /**
     * The sum of all durations of this event pair
     */
    Double sum_duration;
    /**
     * The sum of all completions of this event pair
     */
    Double comp_count;

    public EventPairFrequency(EventPair ev_pair, Double sum_duration, Double comp_count)
    {
        this.ev_pair = ev_pair;
        this.sum_duration = sum_duration;
        this.comp_count = comp_count;
    }

    @Override
    public int compareTo(EventPairFrequency o) //WARNING: Not consistent with equals()
    {
        return Double.compare(this.comp_count, o.comp_count);
    }
    
    public EventPair getEventPair()
    {
        return ev_pair;
    }
    
    public Double getSumDuration()
    {
        return sum_duration;
    }
    
    public Double getCompCount()
    {
        return comp_count;
    }
    
    public double getAvgDuration() 
    {
        return (sum_duration / comp_count);
    }
}
