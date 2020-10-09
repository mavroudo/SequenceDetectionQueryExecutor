package com.sequence.detection.rest.model;

import java.util.Date;

/**
 * Represents a single Detailed Completion returned through the {@code export_completions} endpoint
 * @author Andreas Kosmatopoulos
 */
public class DetailedCompletion
{
    /**
     * The step index of the completed funnel (e.g. in a funnel A->B->C a completion with step index 1 corresponds to a completion of the A->B sub-funnel)
     */
    private int step;
    /**
     * The timestamp of the last event in the completion
     */
    private Date completed_at;
    /**
     * The duration of the funnel
     */
    private long duration;

    public DetailedCompletion(int step, Date completed_at, long duration)
    {
        this.step = step;
        this.completed_at = new Date(completed_at.getTime());
        this.duration = duration;
    }

    public int getStep() {
        return step;
    }

    public void setStep(int step) {
        this.step = step;
    }

    public Date getCompleted_at() {
        return new Date(completed_at.getTime());
    }

    public void setCompleted_at(Date completed_at) {
        this.completed_at = new Date(completed_at.getTime());
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

}
