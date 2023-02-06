package com.datalab.siesta.queryprocessor.model.DBModel;

import java.io.Serializable;
import java.sql.Timestamp;

public class IndexPair implements Serializable {

    private long traceId;
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

    public IndexPair(long traceId, String eventA, String eventB, Timestamp timestampA, Timestamp timestampB) {
        this.traceId = traceId;
        this.positionA = -1;
        this.positionB = -1;
        this.eventA = eventA;
        this.eventB = eventB;
        this.timestampA = timestampA;
        this.timestampB = timestampB;
    }

    public IndexPair(long traceId, String eventA, String eventB, String timestampA, String timestampB) {
        this.traceId = traceId;
        this.positionA = -1;
        this.positionB = -1;
        this.eventA = eventA;
        this.eventB = eventB;
        this.timestampA = Timestamp.valueOf(timestampA);
        this.timestampB = Timestamp.valueOf(timestampB);
    }

    public IndexPair(long traceId, String eventA, String eventB, int positionA, int positionB) {
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

    public long getTraceId() {
        return traceId;
    }

    public void setTraceId(long traceId) {
        this.traceId = traceId;
    }
}
