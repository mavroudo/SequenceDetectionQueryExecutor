package com.datalab.siesta.queryprocessor.model;


import org.apache.spark.sql.Row;

import java.util.List;

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
        this.compression = json.getAs("compression");
        this.events = json.getAs("events");
        this.filename = json.getAs("filename");
        this.has_previous_stored = json.getAs("has_previous_stored");
        this.last_interval = json.getAs("last_interval");
        this.logname = json.getAs("log_name");
        this.lookback = json.getAs("lookback");
        this.mode = json.getAs("mode");
        this.pairs = json.getAs("pairs");
        this.split_every_days = json.getAs("split_every_days");
        this.traces = json.getAs("traces");
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
