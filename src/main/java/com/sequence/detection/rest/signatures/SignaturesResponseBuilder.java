package com.sequence.detection.rest.signatures;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.sequence.detection.rest.CassandraConfiguration;
import com.sequence.detection.rest.model.Funnel;
import com.sequence.detection.rest.query.ResponseBuilder;

public class SignaturesResponseBuilder extends ResponseBuilder {
    private String tableSequence;
    private String tableMetadata;
    private String strategy;
    /**
     * Constructs a ResponseBuilder object. Whenever a new query comes through the application a new ResponseBuilder object is created tailored to the specific query (i.e. specific funnel and start/end dates).
     * The constructor parses the funnel steps and handles the funnel start and end dates.
     *
     * @param cluster                 The Cassandra cluster connection defined by {@link CassandraConfiguration}
     * @param session                 The session to the Cassandra cluster
     * @param ks                      The Cassandra cluster keyspace metadata of the index keyspace
     * @param cassandra_keyspace_name The index keyspace name
     * @param funnel                  The funnel provided by the application
     * @param from                    Funnel start date
     * @param till                    Funnel end date
     */
    public SignaturesResponseBuilder(Cluster cluster, Session session, KeyspaceMetadata ks, String cassandra_keyspace_name, Funnel funnel, String from, String till, String strategy) {
        super(cluster, session, ks, cassandra_keyspace_name, funnel, from, till);
        this.tableLogName=funnel.getLogName().toLowerCase().replace(".", ",").split(",")[0].replace(" ", "_") + "_sign_idx";
        this.tableSequence=funnel.getLogName().toLowerCase().replace(".", ",").split(",")[0].replace(" ", "_") + "_sign_seq";
        this.tableMetadata=funnel.getLogName().toLowerCase().replace(".", ",").split(",")[0].replace(" ", "_") + "_sign_meta";
        this.strategy=strategy;
    }
}
