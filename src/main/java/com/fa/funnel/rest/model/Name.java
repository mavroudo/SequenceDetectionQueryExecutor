package com.fa.funnel.rest.model;

/**
 * A Step Name as provided by the JSON input
 * @author Andreas Kosmatopoulos
 */
public class Name
{
    private String logName;
    private String applicationID;
    private String logType;

    public void setLogName(String logName)
    {
        this.logName = logName;
    }

    public void setApplicationID(String applicationID)
    {
        this.applicationID = applicationID;
    }

    public void setLogType(String logType)
    {
        this.logType = logType;
    }

    public String getLogName() { return this.logName; }

    public String getApplicationID() { return this.applicationID; }

    public String getLogType() { return this.logType; }


    public Name() {}

//    public Name(String logName, String applicationID)
//    {
//        this.logName = logName;
//        this.applicationID = applicationID;
//    }


    @Override
    public String toString()
    {
        return "{ " + "application_id: " + applicationID +
                ", log_name: " + logName +
                ", log_type: " + logType +
                "}";
    }

    public static class Builder
    {
        private final Name name = new Name();

        public Builder logName(String logName)
        {
            name.logName = logName;
            return this;
        }

        public Builder applicationID(String applicationID)
        {
            name.applicationID = applicationID;
            return this;
        }

        public Builder logType(String logType)
        {
            name.logType = logType;
            return this;
        }

        public Name build()
        {
            return name;
        }
    }

}
