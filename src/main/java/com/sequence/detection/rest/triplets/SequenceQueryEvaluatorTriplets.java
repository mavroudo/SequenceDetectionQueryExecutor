package com.sequence.detection.rest.triplets;

import com.datastax.driver.core.*;
import com.sequence.detection.rest.model.*;
import com.sequence.detection.rest.query.SequenceQueryEvaluator;
import com.sequence.detection.rest.query.SequenceQueryHandler;
import io.netty.util.internal.ConcurrentSet;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SequenceQueryEvaluatorTriplets extends SequenceQueryEvaluator {

    private boolean return_all;

    /**
     * Constructor
     *
     * @param cluster                 The cluster instance
     * @param session                 The session instance
     * @param ks                      The keyspace metadata instance
     * @param cassandra_keyspace_name The keyspace name
     */
    public SequenceQueryEvaluatorTriplets(Cluster cluster, Session session, KeyspaceMetadata ks, String cassandra_keyspace_name, boolean return_all) {
        super(cluster, session, ks, cassandra_keyspace_name);
        this.return_all = return_all;
    }


    public Set<String> evaluateQuery(Date start_date, Date end_date, Sequence query, Map<Integer, List<AugmentedDetail>> queryDetails, long maxDuration, String tableLogName) {
        List<QueryTriple> query_triplets = query.getQueryTripletsConcequtive();
        allEventsPerSession = new ConcurrentHashMap<>();
        Set<String> candidates = this.executeQueriesParallel(tableLogName, query_triplets, query, start_date, end_date, queryDetails);
        List<String> allCandidates = new ArrayList<>(candidates);
        return new HashSet<>(allCandidates);

    }

    private Set<String> executeQueriesParallel(String tableLogName, List<QueryTriple> query_triplets,
                                               Sequence query,
                                               Date start_date,
                                               Date end_date, Map<Integer,
            List<AugmentedDetail>> queryDetails) {
        Set<String> results = new ConcurrentSet<>();
        QueryTriple k = query_triplets.get(0);
        ResultSet rs1 = session.execute("SELECT " + "sequences" + " FROM " + cassandra_keyspace_name + "."
                        + tableLogName + " WHERE event1_name = ? AND event2_name = ? AND event3_name = ?",
                k.getFirst().getName(), k.getSecond().getName(), k.getThird_ev().getName());
        Row row1 = rs1.one();
        if (row1!=null) {
            List<String> c = this.handleTripleRow(query, k.getFirst().getName(), k.getSecond().getName(),
                    k.getThird_ev().getName(), start_date, end_date,
                    row1.getList("sequences", String.class), true);
            results.addAll(c);
        }
        query_triplets.subList(1,query_triplets.size()).stream().parallel()
                .map(s -> {
                    ResultSet rs = session.execute("SELECT " + "sequences" + " FROM " + cassandra_keyspace_name + "."
                                    + tableLogName + " WHERE event1_name = ? AND event2_name = ? AND event3_name = ?",
                            s.getFirst().getName(), s.getSecond().getName(), s.getThird_ev().getName());
                    Row row = rs.one();
                    if (row!=null) {
                        return this.handleTripleRow(query, s.getFirst().getName(), s.getSecond().getName(),
                                s.getThird_ev().getName(), start_date, end_date,
                                row.getList("sequences", String.class), true);
                    }
                    return new ArrayList<String>();
                })
                .forEach(results::retainAll);
        return results;
    }

    private List<String> handleTripleRow(Sequence query, String name, String name1,
                                         String name2, Date start_date, Date end_date,
                                         List<String> sequences, boolean putInMap) {
        ArrayList<String> newlist = new ArrayList<String>();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Pattern pattern = Pattern.compile("(.*?)\\(\\((.*)\\)\\)");
        for (String sessionInfo : sequences) {
            Matcher matcher = pattern.matcher(sessionInfo);
            matcher.matches();
            String id = matcher.group(1);
            newlist.add(id);
            if (putInMap && !allEventsPerSession.containsKey(id))
                allEventsPerSession.put(id, Collections.synchronizedList(new ArrayList<>()));
            if (sessionInfo.endsWith("()")) // This is an empty sequence row (should be removed by the filter)
                continue;
            if (sessionInfo.contains("((")) // Then this is an Event pair
            {
                String[] times = matcher.group(2).split(",");
                handleEventPairsSequence(dateFormat, id, times, name, name1, name2,
                        start_date, end_date);
            }
        }
        return newlist;
    }

    private void handleEventPairsSequence(SimpleDateFormat dateFormat,
                                          String id, String[] times, String name, String name1,
                                          String name2, Date start_date, Date end_date) {

        Date time_first = null;
        Date time_second = null;
        Date time_third = null;
        try {
            time_first = dateFormat.parse(times[0]);
            time_second = dateFormat.parse(times[1]);
            time_third = dateFormat.parse(times[2]);
        } catch (ParseException ex) {
            Logger.getLogger(SequenceQueryEvaluatorTriplets.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (allEventsPerSession.containsKey(id)) {
            if (time_first.after(start_date) && time_first.before(end_date))
                allEventsPerSession.get(id).add(new TimestampedEvent(time_first, new Event(name)));
            if (time_second.after(start_date) && time_second.before(end_date))
                allEventsPerSession.get(id).add(new TimestampedEvent(time_second, new Event(name1)));
            if (time_third.after(start_date) && time_third.before(end_date))
                allEventsPerSession.get(id).add(new TimestampedEvent(time_second, new Event(name2)));
        }
    }

}
