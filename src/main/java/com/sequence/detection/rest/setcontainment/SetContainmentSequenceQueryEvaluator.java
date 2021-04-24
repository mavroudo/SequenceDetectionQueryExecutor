package com.sequence.detection.rest.setcontainment;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.sequence.detection.rest.model.AugmentedDetail;
import com.sequence.detection.rest.model.Event;
import com.sequence.detection.rest.model.QueryPair;
import com.sequence.detection.rest.model.Sequence;
import com.sequence.detection.rest.query.SequenceQueryEvaluator;
import com.sequence.detection.rest.query.SequenceQueryHandler;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class SetContainmentSequenceQueryEvaluator extends SequenceQueryHandler {
    private ConcurrentHashMap<QueryPair, List<String>> allIdsPerPair;

    /**
     * Constructor
     *
     * @param cluster                 The cluster instance
     * @param session                 The session instance
     * @param ks                      The keyspace metadata instance
     * @param cassandra_keyspace_name The keyspace name
     */
    public SetContainmentSequenceQueryEvaluator(Cluster cluster, Session session, KeyspaceMetadata ks, String cassandra_keyspace_name) {
        super(cluster, session, ks, cassandra_keyspace_name);

    }

    public Map<QueryPair, List<Long>> getIdsForEveryPair(Date start_date, Date end_date, Sequence query, Map<Integer, List<AugmentedDetail>> allDetails, String tableName) {
        HashMap<QueryPair, List<Long>> allCandidates = new HashMap<>();
        List<QueryPair> query_tuples = query.getQueryTuplesConcequtive();
        if (query_tuples.isEmpty())
            return allCandidates;
        allIdsPerPair = new ConcurrentHashMap<>();
        if (ks.getTable(tableName) == null)
            return allCandidates;

        Map<QueryPair, List<Long>> candidates = executeQuery(tableName, query_tuples, query, start_date, end_date, allDetails);
        allCandidates.putAll(candidates);
        return allCandidates;
    }

    protected Map<QueryPair, List<Long>> executeQuery(String tableName, List<QueryPair> query_tuples, Sequence query, Date start_date, Date end_date, Map<Integer, List<AugmentedDetail>> allDetails) {
        final ExecutorService epThread = Executors.newSingleThreadExecutor();
        final ExecutorService detThread = Executors.newSingleThreadExecutor();

        final CountDownLatch doneSignal; // The countdown will reach zero once all threads have finished their task
        doneSignal = new CountDownLatch(Math.max(query_tuples.size() + allDetails.size() - 1, 0));

        ResultSet rs = session.execute("SELECT " + "sequences" + " FROM " + cassandra_keyspace_name + "." + tableName + " WHERE event1_name = ? AND event2_name = ? ", query_tuples.get(0).getFirst().getName(), query_tuples.get(0).getSecond().getName());
        Row row = rs.one();

        HashMap<QueryPair, List<Long>> firstResults = new HashMap<>();
        if (row != null) {
            List<String> first = row.getList("sequences", String.class);
            firstResults.put(query_tuples.get(0), first.stream().map(Long::parseLong).collect(Collectors.toList()));
        }
        Map<QueryPair, List<Long>> candidates = new ConcurrentHashMap<>(firstResults);

        for (QueryPair ep : query_tuples) // Query (async) for all (but the first) event triples of the query
        {
            if (query_tuples.get(0) == ep)
                continue;

            ResultSetFuture resultSetFuture = session.executeAsync("SELECT " + "sequences" + " FROM " + cassandra_keyspace_name + "." + tableName + " WHERE event1_name = ? AND event2_name = ?", ep.getFirst().getName(), ep.getSecond().getName());
            Futures.addCallback(resultSetFuture, new IdsCallback(candidates, query, ep, start_date, end_date, doneSignal, "sequences"), epThread);
        }

        try {
            doneSignal.await(); // Wait until all async queries have finished
        } catch (InterruptedException ex) {
            Logger.getLogger(SequenceQueryEvaluator.class.getName()).log(Level.SEVERE, null, ex);
        }

        epThread.shutdown();
        detThread.shutdown();

        return candidates;
    }

    public List<Long> verifyPattern(List<Long> candidates, Sequence query, String tableName, Date start_date, Date end_date,String strategy) {
        ArrayList<Long> containQuery = new ArrayList<>();
        if(candidates.isEmpty()){
            return containQuery;
        }
        for(Long candidate: candidates){
            ResultSet rs = session.execute("SELECT " + "events" + " FROM " + cassandra_keyspace_name + "." + tableName + " WHERE sequence_id = ? ", String.valueOf(candidate));
            Row row = rs.one();
            List<String> activities = row.getList("events", String.class);
            List<String> events = this.getEvents(activities,start_date,end_date);
            if(VerifyPattern.verifyPattern(query,events,strategy)){
                containQuery.add(candidate);
            }
        }

        return containQuery;

    }

    private List<String> getEvents(List<String> activities, Date start_date, Date end_date){
        List<String> events = new ArrayList<>();
        List<Date> timestamps = new ArrayList<>();
        for(String activity: activities){
            String[] x = activity.split("\\(")[1].split("\\)")[0].split(",");
            Date date = new Date();
            try {
                date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(x[0]);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            String eventName = x[1];
            events.add(eventName);
            timestamps.add(date);
        }


        int start = 0;
        int end =0;
        for (int i=0;i<timestamps.size();i++){
            if (timestamps.get(i).before(start_date)){
                start=i;
            }else if(timestamps.get(i).after(end_date)){
                end=i;
            }
        }
        if (end ==0){
            end=timestamps.size();
        }
        return events.subList(start,end);
    }


    protected static class IdsCallback implements FutureCallback<ResultSet> {
        Map<QueryPair, List<Long>> candidates;
        Sequence query;
        String first;
        String second;
        Date start_date;
        Date end_date;
        QueryPair ep;
        CountDownLatch doneSignal;
        String ids_field_name;

        public IdsCallback(Map<QueryPair, List<Long>> candidates, Sequence query, QueryPair ep, Date start_date, Date end_date, CountDownLatch doneSignal, String ids_field_name) {
            this.candidates = candidates;
            this.query = query;
            this.first = ep.getFirst().getName();
            this.second = ep.getSecond().getName();
            this.ep = ep;
            this.start_date = start_date;
            this.end_date = end_date;
            this.doneSignal = doneSignal;
            this.ids_field_name = ids_field_name;
        }

        @Override
        public void onSuccess(ResultSet resultSet) {
            Row row = resultSet.one();
            List<String> ids;
            if (row != null) {
                ids = row.getList(ids_field_name, String.class);
            } else
                ids = new ArrayList<String>();

            candidates.put(ep, ids.stream().map(Long::parseLong).collect(Collectors.toList()));
            doneSignal.countDown();

        }

        @Override
        public void onFailure(Throwable throwable) {
            System.out.println("Cassandra query failed! - " + first + "/" + second + "/" + ep);
            System.out.println(throwable.getMessage());
            doneSignal.countDown();
        }
    }

}
