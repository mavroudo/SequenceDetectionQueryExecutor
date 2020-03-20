package com.fa.funnel.rest.model;

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
    /**
     * Session ID of the completion
     */
    private String session_id;
    /**
     * Application ID of the completion
     */
    private String app_id;
    /**
     * Device ID of the completion
     */
    private String device_id;
    /**
     * User ID of the completion
     */
    private String user_id;

    public DetailedCompletion(int step, Date completed_at, long duration, String session_id, String app_id, String device_id, String user_id)
    {
        this.step = step;
        this.completed_at = new Date(completed_at.getTime());
        this.duration = duration;
        this.session_id = session_id;
        this.app_id = app_id;
        this.device_id = device_id;
        this.user_id = user_id;
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

    public String getSession_id() {
        return session_id;
    }

    public void setSession_id(String session_id) {
        this.session_id = session_id;
    }

    public String getApp_id() {
        return app_id;
    }

    public void setApp_id(String app_id) {
        this.app_id = app_id;
    }

    public String getDevice_id() {
        return device_id;
    }

    public void setDevice_id(String device_id) {
        this.device_id = device_id;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }
}
