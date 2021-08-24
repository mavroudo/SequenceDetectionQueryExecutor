package com.sequence.detection.rest.signatures;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.FutureCallback;
import com.sequence.detection.rest.model.Event;
import com.sequence.detection.rest.model.EventPair;
import com.sequence.detection.rest.model.QueryPair;
import com.sequence.detection.rest.model.Sequence;
import com.sequence.detection.rest.query.SequenceQueryHandler;
import com.sequence.detection.rest.util.VerifyPattern;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class Signature extends SequenceQueryHandler {

    private List<EventPair> pairs;
    private List<String> events;
    private String cassandra_keyspace_name;
    private String tableName_idx;
    private String tableName_seq;
    private Session session;


    public Signature(Cluster cluster, Session session, KeyspaceMetadata ks, String cassandra_keyspace_name, String tableName_meta, String tableName_idx, String tableName_seq) {
        super(cluster, session, ks, cassandra_keyspace_name);
        this.cassandra_keyspace_name = cassandra_keyspace_name;
        this.tableName_idx = tableName_idx;
        this.tableName_seq=tableName_seq;
        this.session = session;
        //query the metadata
        ResultSet rs = session.execute("select * from " + cassandra_keyspace_name + "." + tableName_meta);
        List<Row> rows = rs.all();
        this.pairs = new ArrayList<>();
        this.events = new ArrayList<>();
        for (Row row : rows) {
            if (row.getString(0).equals("pairs")) {
                for (String pair : row.getList(1, String.class)) {
                    pairs.add(new EventPair(pair.split(",")[0], pair.split(",")[1]));
                }
            } else if (row.getString(0).equals("events")) {
                events = row.getList(1, String.class);
            }
        }


    }

    private HashSet<Integer> findPositionsWith1(Sequence s) {
        HashSet<Integer> has = new HashSet<>();
        for (Event event : s.getList()) {
            has.add(events.indexOf(event.getName()));
        }
        for (QueryPair qp : s.getQueryTuples()) {
            has.add(pairs.indexOf(new EventPair(qp.getFirst().getName(), qp.getSecond().getName())) + events.size());
        }
        return has;
    }

    private String createQuery(Set<Integer> p) {
        StringBuilder s = new StringBuilder();
        Iterator iter = p.iterator();
        List<String> conditions = new ArrayList<>();
        s.append("Select sequence_ids from ").append(this.cassandra_keyspace_name).append(".").append(this.tableName_idx).append(" where");

        while (iter.hasNext()){
            conditions.add(" signature["+iter.next().toString()+"]="+ "'1' ");
        }
        s.append(String.join("and",conditions)).append("ALLOW FILTERING ;");
        return s.toString();
    }

    public List<Long> executeQuery(Sequence s, Date start_date, Date end_date, String strategy){
        List<Long> candidates = new ArrayList<>();
        String query = createQuery(findPositionsWith1(s));
        System.out.println(query);
        ResultSet rs = this.session.execute(query);
        for (Row r : rs.all()){
            for (String c :r.getList(0,String.class)){
                candidates.add(Long.valueOf(c));
            }
        }


        return this.verifyPattern(candidates,s,this.tableName_seq,start_date,end_date,strategy);

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
