package com.datalab.siesta.queryprocessor.model.DBModel;

import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Events.EventTs;
import com.fasterxml.jackson.annotation.JsonIgnore;


import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A record of the IndexTable. Stores information about:
 * - the trace if
 * - the names of the events (eventA, eventB)
 * - the timestamps of the events (timestampA, timestampB)
 * - the position of the events in the trace (positionA, positionB)
 * Note that depending on the metadata, IndexTable will contain only one of the timestamps/positions. Therefore
 * it is expected the other fields to be empty (null/-1 respectively). If both information is required to answer a
 * query, they can be retrieved from SequenceTable (which contains both).
 */
public class IndexPair implements Serializable {

    private String traceId;
    private String eventA;
    private String eventB;
    private Timestamp timestampA;
    private Timestamp timestampB;
    private int positionA;
    private int positionB;

    public IndexPair() {
        this.eventA = "";
        this.eventB = "";
        this.positionA = -1;
        this.positionB = -1;
        this.timestampA = null;
        this.timestampB = null;
    }

    public IndexPair(String traceId, String eventA, String eventB, Timestamp timestampA, Timestamp timestampB) {
        this.traceId = traceId;
        this.positionA = -1;
        this.positionB = -1;
        this.eventA = eventA;
        this.eventB = eventB;
        this.timestampA = timestampA;
        this.timestampB = timestampB;
    }

    public IndexPair(String traceId, String eventA, String eventB, String timestampA, String timestampB) {
        this.traceId = traceId;
        this.positionA = -1;
        this.positionB = -1;
        this.eventA = eventA;
        this.eventB = eventB;
        this.timestampA = Timestamp.valueOf(timestampA);
        this.timestampB = Timestamp.valueOf(timestampB);
    }

    public IndexPair(String traceId, String eventA, String eventB, int positionA, int positionB) {
        this.traceId = traceId;
        this.timestampA = null;
        this.timestampB = null;
        this.eventA = eventA;
        this.eventB = eventB;
        this.positionA = positionA;
        this.positionB = positionB;
    }

    public String getEventA() {
        return eventA;
    }

    public void setEventA(String eventA) {
        this.eventA = eventA;
    }

    public String getEventB() {
        return eventB;
    }

    public void setEventB(String eventB) {
        this.eventB = eventB;
    }

    public Timestamp getTimestampA() {
        return timestampA;
    }

    public void setTimestampA(Timestamp timestampA) {
        this.timestampA = timestampA;
    }

    public Timestamp getTimestampB() {
        return timestampB;
    }

    public void setTimestampB(Timestamp timestampB) {
        this.timestampB = timestampB;
    }

    public int getPositionA() {
        return positionA;
    }

    public void setPositionA(int positionA) {
        this.positionA = positionA;
    }

    public int getPositionB() {
        return positionB;
    }

    public void setPositionB(int positionB) {
        this.positionB = positionB;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    @JsonIgnore
    public List<Event> getEvents(){
        List<Event> e = new ArrayList<>();
        if(timestampA==null){//the events are pos
            EventPos eventPos1 = new EventPos(this.eventA,this.positionA);
            EventPos eventPos2 = new EventPos(this.eventB,this.positionB);
            eventPos1.setTraceID(this.traceId);
            eventPos2.setTraceID(this.traceId);
            e.add(eventPos1);
            e.add(eventPos2);
        }else{//the events are ts
            EventTs eventTs1 = new EventTs(this.eventA,this.timestampA);
            EventTs eventTs2 = new EventTs(this.eventB,this.timestampB);
            eventTs1.setTraceID(this.traceId);
            eventTs2.setTraceID(this.traceId);
            e.add(eventTs1);
            e.add(eventTs2);
        }
        return e;
    }

    @JsonIgnore
    public boolean validate(Set<EventPair> pairs){
        for(EventPair p:pairs){
            if(p.getEventA().getName().equals(this.eventA)&&p.getEventB().getName().equals(this.eventB)) return true;
        }
        return false;
    }

    public long getDuration() { return (timestampB.getTime() - timestampA.getTime()) / 1000; }
}
