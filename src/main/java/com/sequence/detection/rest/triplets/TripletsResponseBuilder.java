package com.sequence.detection.rest.triplets;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.sequence.detection.rest.model.*;
import com.sequence.detection.rest.model.Responses.DetectedSequenceAllInstances;
import com.sequence.detection.rest.model.Responses.DetectedSequenceNoTime;
import com.sequence.detection.rest.model.Responses.DetectionResponseAllInstances;
import com.sequence.detection.rest.query.ResponseBuilder;
import com.sequence.detection.rest.query.SequenceQueryEvaluator;

import java.util.*;

public class TripletsResponseBuilder extends ResponseBuilder {

    private String tableSequence;

    private boolean return_all;

    /**
     * Constructs a ResponseBuilder object. Whenever a new query comes through the application a new ResponseBuilder object is created tailored to the specific query (i.e. specific funnel and start/end dates).
     * The constructor parses the funnel steps and handles the funnel start and end dates.
     *
     * @param cluster                 The Cassandra cluster connection defined by
     * @param session                 The session to the Cassandra cluster
     * @param ks                      The Cassandra cluster keyspace metadata of the index keyspace
     * @param cassandra_keyspace_name The index keyspace name
     * @param funnel                  The funnel provided by the application
     * @param from                    Funnel start date
     * @param till                    Funnel end date
     */
    public TripletsResponseBuilder(Cluster cluster, Session session, KeyspaceMetadata ks, String cassandra_keyspace_name, Funnel funnel, String from, String till,boolean return_all) {
        super(cluster, session, ks, cassandra_keyspace_name, funnel, from, till);
        this.tableLogName=funnel.getLogName().toLowerCase().replace(".", ",").split(",")[0].replace(" ", "_") + "_trip_idx";
        this.tableSequence=funnel.getLogName().toLowerCase().replace(".", ",").split(",")[0].replace(" ", "_") + "_trip_seq";
        this.return_all=return_all;
    }


    public DetectionResponseAllInstances buildDetectionResponse(){
        long tStart = System.currentTimeMillis();
        List<DetectedSequenceAllInstances> ids = getDetections(steps, start_date, end_date, maxDuration);
        DetectionResponseAllInstances results = new DetectionResponseAllInstances();
        results.setIds(ids);
        long tEnd = System.currentTimeMillis();
        System.out.println("Time Completions (Detection - triplets): " + (tEnd - tStart) / 1000.0 + " seconds.");
        return results;


    }

    private List<DetectedSequenceAllInstances> getDetections(List<Step> steps, Date start_date, Date end_date, long maxDuration) {
        List<List<Step>> listOfSteps = simplifySequences(steps);
        Map<Sequence, Map<Integer, List<AugmentedDetail>>> allQueries = generateAllSubqueriesWithoutAppName(listOfSteps);
        Map<Integer, Map<String, List<Lifetime>>> allTruePositives = new TreeMap<>();
        List<DetectedSequenceAllInstances> detectedSequences = new ArrayList<>();
        for (Map.Entry<Sequence, Map<Integer, List<AugmentedDetail>>> entry : allQueries.entrySet()) {
            SequenceQueryEvaluatorTriplets sqev = new SequenceQueryEvaluatorTriplets(cluster, session, ks, cassandra_keyspace_name,return_all);
            Sequence query = entry.getKey();
            if(query.getList().size()!=steps.size()){ //just return the whole query, without all the subqueries
                continue;
            }
            Map<Integer, List<AugmentedDetail>> queryDetails = entry.getValue();
            if (query.getSize() == 1)
                continue;
            Set<String> candidates = sqev.evaluateQuery(start_date, end_date, query, queryDetails, maxDuration, tableLogName);
            Map<String,List<Lifetime>> truePositives = sqev.evaluateCandidates(query,candidates,maxDuration,return_all,tableLogName);
            detectedSequences.add(new DetectedSequenceAllInstances(truePositives, query.toString()));
            int step = query.getSize() - 1;
            if (!allTruePositives.containsKey(step))
                allTruePositives.put(step, new HashMap<>());
            allTruePositives.get(step).putAll(truePositives);
        }
        return detectedSequences;
    }
}
