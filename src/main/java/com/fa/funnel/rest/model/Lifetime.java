package com.fa.funnel.rest.model;

import java.util.Date;

/**
 * Represents the lifetime of a DetailedCompletion
 * @author Andreas Kosmatopoulos
 */
public class Lifetime 
{
    public Date start_date;
    public Date end_date;
    public long duration;
    public String appID;
    
    public Lifetime(Date start_date, Date end_date, long duration, String appID) 
    {
        this.start_date = new Date(start_date.getTime());
        this.end_date = new Date(end_date.getTime());
        this.duration = duration;
        this.appID = appID;
    }
}
