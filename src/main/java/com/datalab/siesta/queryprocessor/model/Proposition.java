package com.datalab.siesta.queryprocessor.model;

public class Proposition implements Comparable<Proposition>{

    private String event;
    private int completions;
    private double averageDuration;

    public Proposition(String event, int completions, double averageDuration) {
        this.event = event;
        this.completions = completions;
        this.averageDuration = averageDuration;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public int getCompletions() {
        return completions;
    }

    public void setCompletions(int completions) {
        this.completions = completions;
    }

    public double getAverageDuration() {
        return averageDuration;
    }

    public void setAverageDuration(double averageDuration) {
        this.averageDuration = averageDuration;
    }


    @Override
    public int compareTo(Proposition t) {
        double thisScore = (double) this.completions / this.averageDuration;
        double otherScore = (double) t.completions / t.averageDuration;
        if (thisScore > otherScore)
            return +1;
        else if (thisScore < otherScore)
            return -1;
        else
            return t.event.compareTo(this.event);
    }
}
