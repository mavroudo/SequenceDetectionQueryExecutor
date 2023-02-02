package com.datalab.siesta.queryprocessor.model;


import org.apache.spark.sql.Row;


import java.util.Map;

public class Metadata {

    private String compression;
    private Long events;
    private String filename;
    private Boolean has_previous_stored;
    private String last_interval;
    private String logname;
    private Long lookback;
    private String mode;
    private Long pairs;
    private Long split_every_days;
    private Long traces;


    public Metadata(Row json) {
        this.compression = (String) json.getAs("compression");
        this.events = (Long) json.getAs("events");
        this.filename = (String) json.getAs("filename");
        this.has_previous_stored = (Boolean) json.getAs("has_previous_stored");
        this.last_interval = (String) json.getAs("last_interval");
        this.logname = (String) json.getAs("log_name");
        this.lookback = (Long) json.getAs("lookback");
        this.mode = (String) json.getAs("mode");
        this.pairs = (Long) json.getAs("pairs");
        this.split_every_days = (Long) json.getAs("split_every_days");
        this.traces = (Long) json.getAs("traces");
    }

    public Metadata(Map<String,String> attributes){
        this.compression = attributes.get("compression");
        this.events = Long.valueOf(attributes.get("events"));
        this.filename = attributes.get("filename");
        this.has_previous_stored = (Boolean) Boolean.valueOf(attributes.get("has_previous_stored"));
        this.last_interval = attributes.get("last_interval");
        this.logname = attributes.get("log_name");
        this.lookback = Long.valueOf(attributes.get("lookback"));
        this.mode = attributes.get("mode");
        this.pairs = Long.valueOf(attributes.get("pairs"));
        this.split_every_days = Long.valueOf(attributes.get("split_every_days"));
        this.traces = Long.valueOf(attributes.get("traces"));
    }

    public String getCompression() {
        return compression;
    }

    public Long getEvents() {
        return events;
    }

    public String getFilename() {
        return filename;
    }

    public Boolean getHas_previous_stored() {
        return has_previous_stored;
    }

    public String getLast_interval() {
        return last_interval;
    }

    public String getLogname() {
        return logname;
    }

    public Long getLookback() {
        return lookback;
    }

    public String getMode() {
        return mode;
    }

    public Long getPairs() {
        return pairs;
    }

    public Long getSplit_every_days() {
        return split_every_days;
    }

    public Long getTraces() {
        return traces;
    }
}
