package com.datalab.siesta.queryprocessor.model.DBModel;


import org.apache.spark.sql.Row;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * A Metadata object contains all the metadata about a specific log database.
 */
@Getter
public class Metadata {

    /**
     * Compression algorithm used during indexing
     */
    private String compression;
    /**
     * Number of indexed events
     */
    private Long events;
    /**
     * Name of the last indexed logfile
     */
    private String filename;
    /**
     * If this log database has already indexed records, or it is recently initialized
     */
    private Boolean has_previous_stored;
    /**
     * Name of the log database
     */
    private String logname;
    /**
     * That parameter shows what is the maximum allow time-distance between two events in order to construct a valid
     * event-pair
     */
    private Long lookback;
    /**
     * Takes 2 values positions/timestamps. Shows what information is stored in the IndexTable
     *
     * @see com.datalab.siesta.queryprocessor.model.DBModel.IndexPair
     */
    private String mode;
    /**
     * Number of indexed pairs
     */
    private Long pairs;
    /**
     * Number of indexed traces
     */
    private Long traces;

    /**
     * The first timestamp of a log database
     */
    @Setter
    private String start_ts;

    /**
     * The last timestamp of a log database
     */
    private String last_ts;

    /**
     * The last timestamp of the event that was indexed for declare state
     */
    private String last_declare_mined;


    /**
     * Parse a json row. Utilized in S3, as metadata stored in json format
     *
     * @param json metadata in json format
     */
    public Metadata(Row json) {
        this.compression = json.getAs("compression");
        this.events = json.getAs("events");
        this.filename = json.getAs("filename");
        this.has_previous_stored = json.getAs("has_previous_stored");
        this.logname = json.getAs("log_name");
        Integer l = json.getAs("lookback");
        this.lookback = l.longValue();
        this.mode = json.getAs("mode");
        this.pairs = json.getAs("pairs");
        this.traces = json.getAs("traces");
        this.last_declare_mined = json.getAs("last_declare_mined");
        this.last_ts = json.getAs("last_ts");
        this.start_ts = json.getAs("start_ts");
    }

    /**
     * Parse data from a map. Utilized in Cassandra, as metadata stored in a key:value format
     *
     * @param attributes metadata in map format
     */
    public Metadata(Map<String, String> attributes) {
        this.compression = attributes.get("compression");
        this.events = Long.valueOf(attributes.get("events"));
        this.filename = attributes.get("filename");
        this.has_previous_stored = (Boolean) Boolean.valueOf(attributes.get("has_previous_stored"));
        this.logname = attributes.get("log_name");
        this.lookback = Long.valueOf(attributes.get("lookback"));
        this.mode = attributes.get("mode");
        this.pairs = Long.valueOf(attributes.get("pairs"));
        this.traces = Long.valueOf(attributes.get("traces"));
        this.last_declare_mined = attributes.get("last_declare_mined");
        this.last_ts = attributes.get("last_ts");
        this.start_ts = attributes.get("start_ts");
    }

    public Metadata() {
    }
}
