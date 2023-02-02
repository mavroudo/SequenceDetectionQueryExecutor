package com.datalab.siesta.queryprocessor.storage.repositories.S3;

public class CountRecords {

    private String eventB;

    private long sum_duration;

    private int count;

    private long min_duration;

    private long max_duration;

    public CountRecords() {
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
}
