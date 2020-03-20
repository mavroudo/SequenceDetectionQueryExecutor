package com.fa.funnel.rest.model;

/**
 * Represents a Proposition corresponding to a continuation provided by the {@code explore/*} endpoint
 * @author Andreas Kosmatopoulos
 */
public class Proposition implements Comparable<Proposition>
{
    private int applicationID;
    private int logType;
    private String logName;
    private int completions;
    private Double averageDuration;
    private String lastCompleted;

    public Proposition(int applicationID, int logType, String logName, int completions) {
        this.applicationID = applicationID;
        this.logType = logType;
        this.logName = logName;
        this.completions = completions;
    }

    public Proposition(int applicationID, int logType, String logName, int completions, Double averageDuration) {
        this.applicationID = applicationID;
        this.logType = logType;
        this.logName = logName;
        this.completions = completions;
        this.averageDuration = averageDuration;
    }
    
    public Proposition(int applicationID, int logType, String logName, int completions, Double averageDuration, String lastCompleted) {
        this.applicationID = applicationID;
        this.logType = logType;
        this.logName = logName;
        this.completions = completions;
        this.averageDuration = averageDuration;
        this.lastCompleted = lastCompleted;
    }

    public int getApplicationID() {
        return applicationID;
    }

    public void setApplicationID(int applicationID) {
        this.applicationID = applicationID;
    }

    public int getLogType() {
        return logType;
    }

    public void setLogType(int logType) {
        this.logType = logType;
    }

    public String getLogName() {
        return logName;
    }

    public void setLogName(String logName) {
        this.logName = logName;
    }

    public int getCompletions() {
        return completions;
    }

    public void setCompletions(int completions) {
        this.completions = completions;
    }

    public Double getAverageDuration() {
        return averageDuration;
    }

    public void setAverageDuration(Double averageDuration) {
        this.averageDuration = averageDuration;
    }

    public String getLastCompleted() {
        return lastCompleted;
    }

    public void setLastCompleted(String lastCompleted) {
        this.lastCompleted = lastCompleted;
    }

    @Override
    public String toString() {
        return "{" +
                "applicationID=" + applicationID +
                ", logType=" + logType +
                ", logName='" + logName + '\'' +
                ", completions=" + completions +
                ", averageDuration=" + averageDuration +
                ", lastCompleted='" + lastCompleted + '\'' +
                '}';
    }

    @Override
    public int compareTo(Proposition t)
    {
        double thisScore = (double) this.completions / this.averageDuration;
        double otherScore = (double) t.completions / t.averageDuration;
        if (thisScore > otherScore)
            return -1;
        else if (thisScore < otherScore)
            return +1;
        else
            return t.logName.compareTo(this.logName);

    }
}
