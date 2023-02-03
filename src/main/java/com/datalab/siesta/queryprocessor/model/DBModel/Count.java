package com.datalab.siesta.queryprocessor.model.DBModel;

import java.io.Serializable;

public class Count implements Serializable {

    private String eventA;

    private String eventB;

    private long sum_duration;

    private int count;

    private long min_duration;

    private long max_duration;

    public Count() {
    }

    public Count(String eventA, String eventB, long sum_duration, int count, long min_duration, long max_duration) {
        this.eventA = eventA;
        this.eventB = eventB;
        this.sum_duration = sum_duration;
        this.count = count;
        this.min_duration = min_duration;
        this.max_duration = max_duration;
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

    public long getSum_duration() {
        return sum_duration;
    }

    public void setSum_duration(long sum_duration) {
        this.sum_duration = sum_duration;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public long getMin_duration() {
        return min_duration;
    }

    public void setMin_duration(long min_duration) {
        this.min_duration = min_duration;
    }

    public long getMax_duration() {
        return max_duration;
    }

    public void setMax_duration(long max_duration) {
        this.max_duration = max_duration;
    }

    @Override
    public String toString() {
        return "Count{" +
                "eventA='" + eventA + '\'' +
                ", eventB='" + eventB + '\'' +
                ", sum_duration=" + sum_duration +
                ", count=" + count +
                ", min_duration=" + min_duration +
                ", max_duration=" + max_duration +
                '}';
    }
}
