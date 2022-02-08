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
import com.sequence.detection.rest.util.VerifyPattern;

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

    public Map<Event, List<Long>> getIdsForEveryPair(Date start_date, Date end_date, Sequence query, Map<Integer, List<AugmentedDetail>> allDetails, String tableName) {
        ArrayList<Event> queryEvents = query.getList();
        HashMap<Event,List<Long>> allCandidates = new HashMap<>();
        if (queryEvents.isEmpty())
            return allCandidates;
        Map<Event, List<Long>> candidates2 = executeQuery(tableName, queryEvents, query, start_date, end_date, allDetails);
        allCandidates.putAll(candidates2);
        return allCandidates;
    }

    protected Map<Event, List<Long>> executeQuery(String tableName, List<Event> events, Sequence query, Date start_date, Date end_date, Map<Integer, List<AugmentedDetail>> allDetails) {
        final ExecutorService epThread = Executors.newSingleThreadExecutor();
        final ExecutorService detThread = Executors.newSingleThreadExecutor();

        final CountDownLatch doneSignal; // The countdown will reach zero once all threads have finished their task
        doneSignal = new CountDownLatch(Math.max(events.size() + allDetails.size() - 1, 0));

        ResultSet rs = session.execute("SELECT " + "sequences" + " FROM " + cassandra_keyspace_name + "." + tableName + " WHERE event_name = ?", events.get(0).getName());
        Row row = rs.one();

        HashMap<Event, List<Long>> firstResults = new HashMap<>();
        if (row != null) {
            List<String> first = row.getList("sequences", String.class);
            firstResults.put(events.get(0), first.stream().map(Long::parseLong).collect(Collectors.toList()));
        }
        Map<Event, List<Long>> candidates = new ConcurrentHashMap<>(firstResults);

        for (Event e : events) // Query (async) for all (but the first) event triples of the query
        {
            if (events.get(0) == e)
                continue;

            ResultSetFuture resultSetFuture = session.executeAsync("SELECT " + "sequences" + " FROM " + cassandra_keyspace_name + "." + tableName + " WHERE event_name = ?", e.getName());
            Futures.addCallback(resultSetFuture, new IdsCallback(candidates, query, e, start_date, end_date, doneSignal, "sequences"), epThread);
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
        Map<Event, List<Long>> candidates;
        Sequence query;
        String name;
        Date start_date;
        Date end_date;
        Event e;
        CountDownLatch doneSignal;
        String ids_field_name;

        public IdsCallback(Map<Event, List<Long>> candidates, Sequence query, Event e, Date start_date, Date end_date, CountDownLatch doneSignal, String ids_field_name) {
            this.candidates = candidates;
            this.query = query;
            this.name= e.getName();
            this.e = e;
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

            candidates.put(e, ids.stream().map(Long::parseLong).collect(Collectors.toList()));
            doneSignal.countDown();

        }

        @Override
        public void onFailure(Throwable throwable) {
            System.out.println("Cassandra query failed! - " + name + "/" + e);
            System.out.println(throwable.getMessage());
            doneSignal.countDown();
        }
    }

}
