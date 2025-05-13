package com.datalab.siesta.queryprocessor.model;

import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.fasterxml.jackson.annotation.JsonIgnore;


import java.util.List;

/**
 * A single occurrence of the pattern in a trace. It is consisted of a list of all the events that correspond to the
 * pattern occurrence
 */
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

    /**
     * Determines if this occurrence overlaps in time with another one
     * @param occurrence another occurrence
     * @return true if these occurrences overlap and false otherwise
     */
    public boolean overlaps(Occurrence occurrence){
        EventBoth fEvent = occurrence.getOccurrence().get(0);
        EventBoth lEvent = occurrence.getOccurrence().get(occurrence.getOccurrence().size()-1);
        int size = this.occurrence.size();
        boolean notOverlaps;
        if(fEvent.getTimestamp()==null){ // we are handling positions
            notOverlaps = this.occurrence.get(size-1).getPosition()<fEvent.getPosition() ||
                    this.occurrence.get(0).getPosition()>lEvent.getPosition();
        }else{
            notOverlaps = this.occurrence.get(size-1).getTimestamp().before(fEvent.getTimestamp()) ||
                    this.occurrence.get(0).getTimestamp().after(lEvent.getTimestamp());
        }
        return !notOverlaps;
    }

    /**
     * @return the duration in seconds, where duration is the difference between the first and last event
     */
    @JsonIgnore
    public double getDuration(){
        int s = occurrence.size();
        if(occurrence.get(0).getTimestamp()!=null){
            return ((double) occurrence.get(s-1).getTimestamp().getTime()-occurrence.get(0).getTimestamp().getTime())/1000;
        }
        return 0;
    }
}
