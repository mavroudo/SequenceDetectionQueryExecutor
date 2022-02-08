package com.sequence.detection.rest.signatures;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.sequence.detection.rest.CassandraConfiguration;
import com.sequence.detection.rest.model.*;
import com.sequence.detection.rest.query.ResponseBuilder;
import com.sequence.detection.rest.model.DetectedSequenceNoTime;
import com.sequence.detection.rest.model.DetectionResponseNoTime;
import com.sequence.detection.rest.setcontainment.LCJoin;
import com.sequence.detection.rest.setcontainment.SetContainmentSequenceQueryEvaluator;

import java.util.*;

public class SignaturesResponseBuilder extends ResponseBuilder {
    private String tableSequence;
    private String tableMetadata;
    private String strategy;
    private Signature signature;

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
        this.tableLogName = funnel.getLogName().toLowerCase().replace(".", ",").split(",")[0].replace(" ", "_") + "_sign_idx";
        this.tableSequence = funnel.getLogName().toLowerCase().replace(".", ",").split(",")[0].replace(" ", "_") + "_sign_seq";
        this.tableMetadata = funnel.getLogName().toLowerCase().replace(".", ",").split(",")[0].replace(" ", "_") + "_sign_meta";
        this.strategy = strategy;
        //initialize the signature (will query the meta and hold it to create the signatures faster)
        this.signature = new Signature(this.cluster, this.session, ks, this.cassandra_keyspace_name, this.tableMetadata, this.tableLogName,this.tableSequence);
    }

    public DetectionResponseNoTime buildDetectionResponseNoTime() {
        long tStart = System.currentTimeMillis();




        List<DetectedSequenceNoTime> ids = getDetections(steps, start_date, end_date, maxDuration, tableLogName, strategy);

        long tEnd = System.currentTimeMillis();
        System.out.println("Time Completions (Detection - with Signatures): " + (tEnd - tStart) / 1000.0 + " seconds.");


        DetectionResponseNoTime result = new DetectionResponseNoTime();
        result.setIds(ids);

        return result;
    }

    private List<DetectedSequenceNoTime> getDetections(List<Step> steps, Date start_date, Date end_date, long maxDuration, String tableName, String strategy) {
        List<DetectedSequenceNoTime> detectedSequences = new ArrayList<>();
        List<List<Step>> listOfSteps = simplifySequences(steps);
        Map<Sequence, Map<Integer, List<AugmentedDetail>>> allQueries = generateAllSubqueriesWithoutAppName(listOfSteps);

        for (Map.Entry<Sequence, Map<Integer, List<AugmentedDetail>>> entry : allQueries.entrySet()) {
            Sequence query = entry.getKey();

            if(query.getList().size()!=steps.size()){ //just return the whole query, without all the subqueries
                continue;
            }
            if (query.getSize() == 1)
                continue;
            List<Long> ids  = this.signature.executeQuery(query, start_date, end_date, strategy);
            detectedSequences.add(new DetectedSequenceNoTime(ids,query.toString()));
        }
        return detectedSequences;

    }

}
