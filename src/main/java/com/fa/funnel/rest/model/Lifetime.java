package com.fa.funnel.rest.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Represents the lifetime of a DetailedCompletion
 * @author Andreas Kosmatopoulos
 */
public class Lifetime 
{
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
    public Date start_date;
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
    public Date end_date;
    public long duration;
    @JsonIgnore
    public String appID;
    
    public Lifetime(Date start_date, Date end_date, long duration, String appID) 
    {

        this.start_date = new Date(start_date.getTime());
        this.end_date = new Date(end_date.getTime());
        this.duration = duration;
        this.appID = appID;
    }

    @Override
    public String toString() {
        DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
        StringBuilder json = new StringBuilder("{ ");
        json.append("from : "+dateFormat.format(start_date)+",");
        json.append("until : "+dateFormat.format(end_date)+",");
        json.append("duration : "+duration+"}");
        return json.toString();
    }
}
