package com.sequence.detection.rest.setcontainment;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.sequence.detection.rest.CassandraConfiguration;
import com.sequence.detection.rest.model.*;
import com.sequence.detection.rest.query.ResponseBuilder;
import com.sequence.detection.rest.query.SequenceQueryEvaluator;

import java.util.*;

public class SetContainmentResponseBuilder extends ResponseBuilder {
    private String tableSequence;

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
    public SetContainmentResponseBuilder(Cluster cluster, Session session, KeyspaceMetadata ks, String cassandra_keyspace_name, Funnel funnel, String from, String till) {
        super(cluster, session, ks, cassandra_keyspace_name, funnel, from, till);
        this.tableLogName=funnel.getLogName().toLowerCase().replace(".", ",").split(",")[0].replace(" ", "_") + "_set_idx";
        this.tableSequence=funnel.getLogName().toLowerCase().replace(".", ",").split(",")[0].replace(" ", "_") + "_set_seq";

    }

    public DetectionResponse buildDetectionResponse(){
        long tStart = System.currentTimeMillis();

        long tEnd = System.currentTimeMillis();
        System.out.println("Time Completions (Detection - with Set Containment): " + (tEnd - tStart) / 1000.0 + " seconds.");


        List<DetectedSequence> ids = getDetections(steps, start_date, end_date, maxDuration, tableLogName);


        DetectionResponse result = new DetectionResponse();
        result.setIds(ids);

        return result;
    }

    private List<DetectedSequence> getDetections(List<Step> steps, Date start_date, Date end_date, long maxDuration, String tableName){
        List<List<Step>> listOfSteps = simplifySequences(steps);
        Map<Sequence, Map<Integer, List<AugmentedDetail>>> allQueries = generateAllSubqueriesWithoutAppName(listOfSteps);
        List<DetectedSequence> detectedSequences = new ArrayList<>();

        for (Map.Entry<Sequence, Map<Integer, List<AugmentedDetail>>> entry : allQueries.entrySet()) {
            Sequence query = entry.getKey();
            List<QueryPair> queryTuples = query.getQueryTuplesConcequtive();
            Map<Integer, List<AugmentedDetail>> queryDetails = entry.getValue();

            if (query.getSize() == 1)
                continue;
            SetContainmentSequenceQueryEvaluator sqev = new SetContainmentSequenceQueryEvaluator(cluster, session, ks, cassandra_keyspace_name);

            Map<QueryPair,List<Long>> inverted_lists = sqev.getIdsForEveryPair(start_date, end_date, query, queryDetails, tableName);
            List<Long> candidates = LCJoin.crossCuttingBasedIntersection( new ArrayList<List<Long>>(inverted_lists.values()) );

            sqev.verifyPattern(candidates,query,this.tableSequence,start_date,end_date,"skitillnextmatch");

            System.out.println("hello");

        }
        return new ArrayList<DetectedSequence>();
    }

}
