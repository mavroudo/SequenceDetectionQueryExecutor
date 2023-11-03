package com.datalab.siesta.queryprocessor.model;

/**
 * Contains a possible next continuation of the query pattern, as well as the number of occurrences of the complete
 * pattern (either accurate or approximate) and the average duration of the complete pattern
 */
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


    /**
     * Defines a comparison between 2 propositions so a list of them can be sorted based on this.
     * The scores = total_completions/average_duration (but can be modified depending on the needs)
     * @param t the object to be compared.
     * @return the comparison
     */
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
