package com.datalab.siesta.queryprocessor.Signatures;

import com.datalab.siesta.queryprocessor.model.DBModel.EventTypes;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class represents the Signature as extracted from all the log file. More specifically, it contains
 * a list of all the event types and a list of the most frequent event pairs.
 * This Signature is directly stored inside the storage and needs to be extracted at the beginning.
 * For a given query, this class is utilized to extract the signature and then proceed in comparing the signature
 * with the signatures of all the traces stored.
 */
public class Signature {

    private List<EventTypes> eventPairs;
    private List<String> events;

    public Signature() {
        eventPairs=new ArrayList<>();
        events=new ArrayList<>();
    }


    public Set<Integer> findPositionsWith1(Set<String> eventTypes , Set<EventPair> pairs){
        Set<Integer> has = new HashSet<>();
        for(String e: eventTypes){
            has.add(this.events.indexOf(e));
        }
        for(EventPair ep: pairs){
            int i = this.eventPairs.indexOf(new EventTypes(ep.getEventA().getName(),ep.getEventB().getName()));
            if(i!=-1) has.add(i+this.events.size());
        }
        return has;
    }

    public Signature(List<EventTypes> eventPairs, List<String> events) {
        this.eventPairs = eventPairs;
        this.events = events;
    }

    public List<EventTypes> getEventPairs() {
        return eventPairs;
    }

    public void setEventPairs(List<EventTypes> eventPairs) {
        this.eventPairs = eventPairs;
    }

    public List<String> getEvents() {
        return events;
    }

    public void setEvents(List<String> events) {
        this.events = events;
    }
}


