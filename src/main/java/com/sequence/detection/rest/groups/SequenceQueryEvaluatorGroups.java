package com.sequence.detection.rest.groups;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.sequence.detection.rest.query.SequenceQueryHandler;

import java.util.List;

public class SequenceQueryEvaluatorGroups extends SequenceQueryHandler {
    private List<List<Integer>> groups;

    /**
     * Constructor
     *
     * @param cluster                 The cluster instance
     * @param session                 The session instance
     * @param ks                      The keyspace metadata instance
     * @param cassandra_keyspace_name The keyspace name
     */
    public SequenceQueryEvaluatorGroups(Cluster cluster, Session session, KeyspaceMetadata ks, String cassandra_keyspace_name, List<List<Integer>> groups) {
        super(cluster, session, ks, cassandra_keyspace_name);
        this.groups=groups;
    }


}
