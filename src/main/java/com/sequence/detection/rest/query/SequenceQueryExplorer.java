package com.sequence.detection.rest.query;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.sequence.detection.rest.model.*;

/**
 * The class responsible for performing funnel exploration. In the case of accurate exploration each continuation is evaluated precisely using the
 * index tables while in the case of an fast exploration the class makes use of the count tables
 * @author Andreas Kosmatopoulos
 */
public class SequenceQueryExplorer extends SequenceQueryEvaluator
{

    /**
     * Constructor
     * @param cluster The cluster instance
     * @param session The session instance
     * @param ks The keyspace metadata instance
     * @param cassandra_keyspace_name The keyspace name
     */
    public SequenceQueryExplorer(Cluster cluster, Session session, KeyspaceMetadata ks, String cassandra_keyspace_name)
    {
        super(cluster, session, ks, cassandra_keyspace_name);
    }

}

