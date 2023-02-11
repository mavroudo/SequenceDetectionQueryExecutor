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

    public boolean overlaps(Occurrence occurrence){
        EventBoth fEvent = occurrence.getOccurrence().get(0);
        EventBoth lEvent = occurrence.getOccurrence().get(occurrence.getOccurrence().size()-1);
        int size = this.occurrence.size();
        if(fEvent.getTimestamp()==null){ // we are handling positions
            return this.occurrence.get(size-1).getPosition()<fEvent.getPosition() ||
                    this.occurrence.get(0).getPosition()>lEvent.getPosition();
        }else{
            return this.occurrence.get(size-1).getTimestamp().before(fEvent.getTimestamp()) ||
                    this.occurrence.get(0).getTimestamp().after(lEvent.getTimestamp());
        }
    }
}
