package com.sequence.detection.rest.query;

import com.datastax.driver.core.*;
import com.sequence.detection.rest.model.Event;
import com.sequence.detection.rest.model.Sequence;
import com.sequence.detection.rest.model.TimestampedEvent;
import com.google.common.util.concurrent.FutureCallback;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class designed to handle the low-level needs of a query evaluation. Each time a query fetches data from the index (not count) tables the incoming rows are handled by this class first.
 * Each row is handled by extracting info about the entity ) ID that achieved the completion and then handling the completion itself in one of two different ways (i.e. as an event pair or as a detail triplet)
 *
 * @author Andreas Kosmatopoulos
 */
public class SequenceQueryHandler {

    /**
     * The cluster instance
     */
    protected Cluster cluster;

    /**
     * The session instance
     */
    protected Session session;

    /**
     * The keyspace metadata instance
     */
    protected KeyspaceMetadata ks;

    /**
     * The keyspace name
     */
    protected String cassandra_keyspace_name;

    /**
     * The delimiter used in the contents of a row
     */
    protected static final String DELAB_DELIMITER = "¦delab¦";

    /**
     * A ConcurrentHashMap that contains for all entities solution candidates the events that this entity has performed
     */
    protected static ConcurrentHashMap<String, List<TimestampedEvent>> allEventsPerSession;

    /**
     * Constructor
     *
     * @param cluster                 The cluster instance
     * @param session                 The session instance
     * @param ks                      The keyspace metadata instance
     * @param cassandra_keyspace_name The keyspace name
     */
    public SequenceQueryHandler(Cluster cluster, Session session, KeyspaceMetadata ks, String cassandra_keyspace_name) {
        this.cluster = cluster;
        this.session = session;
        this.ks = ks;
        this.cassandra_keyspace_name = cassandra_keyspace_name;
    }




    protected static List<String> handleSequenceRow(Sequence query, String first, String second, Date start_date, Date end_date, List<String> candSessions, boolean putInMap) {
        ArrayList<String> newlist = new ArrayList<String>();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Pattern pattern = Pattern.compile("(.*?)\\(\\((.*)\\)\\)");
        for (String sessionInfo : candSessions) {
            Matcher matcher = pattern.matcher(sessionInfo);
            matcher.matches();
            String id = matcher.group(1);
            newlist.add(id);

            if (putInMap && !allEventsPerSession.containsKey(id))
                allEventsPerSession.put(id, Collections.synchronizedList(new ArrayList<TimestampedEvent>()));

            if (sessionInfo.endsWith("()")) // This is an empty sequence row (should be removed by the filter)
                continue;
            if (sessionInfo.contains("((")) // Then this is an Event pair
            {
                String[] times = matcher.group(2).split(",");
                handleEventPairsSequence(dateFormat, id, times, first, second, start_date, end_date);
            }
        }
        return newlist;

    }

    private static void handleEventPairsSequence(SimpleDateFormat dateFormat, String id, String[] times, String first, String second, Date start_date, Date end_date) {
        Date time_first = null;
        Date time_second = null;
        try {
            time_first = dateFormat.parse(times[0]);
            time_second = dateFormat.parse(times[1]);
        } catch (ParseException ex) {
            Logger.getLogger(SequenceQueryEvaluator.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (allEventsPerSession.containsKey(id)) {
            if (time_first.after(start_date) && time_first.before(end_date))
                allEventsPerSession.get(id).add(new TimestampedEvent(time_first, new Event(first)));
            if (time_second.after(start_date) && time_second.before(end_date))
                allEventsPerSession.get(id).add(new TimestampedEvent(time_second, new Event(second)));
        }
    }


    protected static class CandSessionsCallback implements FutureCallback<ResultSet> {
        Collection<String> candidates;
        Sequence query;
        String first;
        String second;
        String third;
        Date start_date;
        Date end_date;
        CountDownLatch doneSignal;
        String ids_field_name;

        public CandSessionsCallback(Collection<String> candidates, Sequence query, String first, String second, Date start_date, Date end_date, CountDownLatch doneSignal, String ids_field_name) {
            this.candidates = candidates;
            this.query = query;
            this.first = first;
            this.second = second;
            this.start_date = start_date;
            this.end_date = end_date;
            this.doneSignal = doneSignal;
            this.ids_field_name = ids_field_name;
        }

        @Override
        public void onSuccess(ResultSet result) {
            Row row = result.one();

            List<String> candSessions;
            if (row != null) {
                candSessions = row.getList(ids_field_name, String.class);
                candSessions = handleSequenceRow(query, first, second, start_date, end_date, candSessions, false);
            } else
                candSessions = new ArrayList<String>();

            candidates.retainAll(new HashSet<String>(candSessions));

            doneSignal.countDown();
        }

        @Override
        public void onFailure(Throwable t) {
            System.out.println("Cassandra query failed! - " + first + "/" + second + "/" + third);
            System.out.println(t.getMessage());
            doneSignal.countDown();
        }
    }
}
