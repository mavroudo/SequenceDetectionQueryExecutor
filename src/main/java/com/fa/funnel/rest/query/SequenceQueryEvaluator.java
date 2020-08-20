package com.fa.funnel.rest.query;

import com.datastax.driver.core.*;
import com.fa.funnel.rest.model.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The class responsible for accurately evaluating a query. Each query is evaluated on a per month basis and then all monthly results are
 * aggregated in a single candidate list. All the initial candidates are then tested and any false positives are discarded
 *
 * @author Andreas Kosmatopoulos
 */
public class SequenceQueryEvaluator extends SequenceQueryHandler {

    /**
     * Constructor
     *
     * @param cluster                 The cluster instance
     * @param session                 The session instance
     * @param ks                      The keyspace metadata instance
     * @param cassandra_keyspace_name The keyspace name
     */
    public SequenceQueryEvaluator(Cluster cluster, Session session, KeyspaceMetadata ks, String cassandra_keyspace_name) {
        super(cluster, session, ks, cassandra_keyspace_name);
    }

    /**
     * Overrides the function below and executes queries only in a table, provided from the funnel
     *
     * @param start_date Funnel start date
     * @param end_date   Funnel end date
     * @param query      Query to be executed
     * @param allDetails The details accompanying the query
     * @param tableName  The name of the log file
     * @return a set of potential candidate user/device IDs
     */
    public Set<String> evaluateQueryLogFile(Date start_date, Date end_date, Sequence query, Map<Integer, List<AugmentedDetail>> allDetails, String tableName) {
        List<String> allCandidates = new ArrayList<String>();
        List<QueryPair> query_tuples = query.getQueryTuples();
        if (query_tuples.isEmpty())
            return new HashSet<String>(allCandidates); // Which is empty
        allEventsPerSession = new ConcurrentHashMap<String, List<TimestampedEvent>>();
        if (ks.getTable(tableName) == null) // If the keyspace doesn't contain the corresponding table, return empty
            return new HashSet<String>(allCandidates); // Which is empty

        Set<String> candidates = executeQuery(tableName, query_tuples, query, start_date, end_date, allDetails);

        allCandidates.addAll(candidates);
        return new HashSet<String>(allCandidates);
    }


    private Set<String> executeQuery(String tableName, List<QueryPair> query_tuples, Sequence query, Date start_date, Date end_date, Map<Integer, List<AugmentedDetail>> allDetails) {
        final ExecutorService epThread = Executors.newSingleThreadExecutor();
        final ExecutorService detThread = Executors.newSingleThreadExecutor();

        final CountDownLatch doneSignal; // The countdown will reach zero once all threads have finished their task
        doneSignal = new CountDownLatch(Math.max(query_tuples.size() + allDetails.size() - 1, 0));

        ResultSet rs = session.execute("SELECT " + "sequences" + " FROM " + cassandra_keyspace_name + "." + tableName + " WHERE event1_name = ? AND event2_name = ? ", query_tuples.get(0).getFirst().getName(), query_tuples.get(0).getSecond().getName());

        Row row = rs.one();

        List<String> candSessionsFull, candSessions;
        if (row != null) {
            candSessionsFull = row.getList("sequences", String.class);
            candSessions = handleSequenceRow(query, query_tuples.get(0).getFirst().getName(), query_tuples.get(0).getSecond().getName(), start_date, end_date, candSessionsFull, true);
        } else
            candSessions = new ArrayList<String>();
        Set<String> candidates = Collections.synchronizedSet(new HashSet<String>(candSessions));
        for (QueryPair ep : query_tuples) // Query (async) for all (but the first) eventtriples of the query
        {
            if (query_tuples.get(0) == ep)
                continue;

            ResultSetFuture resultSetFuture = session.executeAsync("SELECT " + "sequences" + " FROM " + cassandra_keyspace_name + "." + tableName + " WHERE event1_name = ? AND event2_name = ?", ep.getFirst().getName(), ep.getSecond().getName());
            Futures.addCallback(resultSetFuture, new CandSessionsCallback(candidates, query, ep.getFirst().getName(), ep.getSecond().getName(), start_date, end_date, doneSignal, "sequences"), epThread);
        }
        for (Integer stepCount : allDetails.keySet()) {
            List<AugmentedDetail> stepDetails = allDetails.get(stepCount);
            for (AugmentedDetail det : stepDetails) // Query (async) for all the details of the funnel
            {
                ResultSetFuture resultSetFuture = session.executeAsync("SELECT " + "sequences" + " FROM " + cassandra_keyspace_name + "." + tableName + " WHERE event1_name = ? AND event2_name = ? AND third_field = ?", det.getFirst(), det.getSecond(), det.getThird());
                Futures.addCallback(resultSetFuture, new CandSessionsCallback(candidates, query, det.getFirst(), det.getSecond(), start_date, end_date, doneSignal, "sequences"), detThread);
            }
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

    /**
     * The primary function of this class. It executes the query on all months between the user-provided timeframe, aggregates the results
     * and presents a list of potential candidates. The candidates represent user IDs if the funnel was defined as cross-app and device ID otherwise
     *
     * @param start_date   Funnel start date
     * @param end_date     Funnel end date
     * @param query        Query to be executed
     * @param allDetails   The details accompanying the query
     * @param isUsersQuery Boolean representing if the query is cross-app or not
     * @return a set of potential candidate user/device IDs
     */
    public Set<String> evaluateQuery(Date start_date, Date end_date, Sequence query, Map<Integer, List<AugmentedDetail>> allDetails, boolean isUsersQuery) {
        List<String> year_months = getYearMonths(start_date, end_date); // Returns all months and years between "start_date" and "end_date", e.g. "2017_06", "2017_07", ..., "2017_10"
        List<QueryPair> query_tuples = query.getQueryTuples();

        List<String> allCandidates = new ArrayList<String>(); // This stores all the candidates for all months in the date range, combined

        if (query_tuples.isEmpty())
            return new HashSet<String>(allCandidates); // Which is empty

        allEventsPerSession = new ConcurrentHashMap<String, List<TimestampedEvent>>();
        for (String year_month : year_months) {
            String tableName = (!isUsersQuery ? "dvc_idx_" + year_month : "usr_idx_" + year_month); //creates table name
            if (ks.getTable(tableName) == null) // If the keyspace doesn't contain the corresponding table, move on to the next month
                continue;

            Set<String> monthCandidates = evaluateQueryOnMonth(tableName, query_tuples, query, start_date, end_date, allDetails, isUsersQuery);

            allCandidates.addAll(monthCandidates);
        }

        return new HashSet<String>(allCandidates);
    }

    private Set<String> evaluateQueryOnMonth(String tableName, List<QueryPair> query_tuples, Sequence query, Date start_date, Date end_date, Map<Integer, List<AugmentedDetail>> allDetails, boolean isUsersQuery) {
        final ExecutorService epThread = Executors.newSingleThreadExecutor();
        final ExecutorService detThread = Executors.newSingleThreadExecutor();

        final CountDownLatch doneSignal; // The countdown will reach zero once all threads have finished their task
        doneSignal = new CountDownLatch(Math.max(query_tuples.size() + allDetails.size() - 1, 0));

        String ids_field_name = (!isUsersQuery ? "devices" : "users");

        //FIXME: Handle prepared statements better to avoid warning. Atm not using any
//        PreparedStatement statement = session.prepare("SELECT sessions FROM " + cassandra_keyspace_name + "." + tableName + " WHERE first_field = ? AND second_field = ? AND third_field = ?");

        ResultSet rs = session.execute("SELECT " + ids_field_name + " FROM " + cassandra_keyspace_name + "." + tableName + " WHERE first_field = ? AND second_field = ? AND third_field = 'null'", query_tuples.get(0).getFirst().getName(), query_tuples.get(0).getSecond().getName());

        Row row = rs.one();

        List<String> candSessionsFull, candSessions;
        if (row != null) {
            candSessionsFull = row.getList(ids_field_name, String.class);
            candSessions = handleSessionsRow(query, query_tuples.get(0).getFirst().getName(), query_tuples.get(0).getSecond().getName(), start_date, end_date, candSessionsFull, true);
        } else
            candSessions = new ArrayList<String>();

        Set<String> monthCandidates = Collections.synchronizedSet(new HashSet<String>(candSessions)); // If this is the actual query (candidates) use a set for fast retainAll()
        for (QueryPair ep : query_tuples) // Query (async) for all (but the first) eventtriples of the query
        {
            if (query_tuples.get(0) == ep)
                continue;

            ResultSetFuture resultSetFuture = session.executeAsync("SELECT " + ids_field_name + " FROM " + cassandra_keyspace_name + "." + tableName + " WHERE first_field = ? AND second_field = ? AND third_field = 'null'", ep.getFirst().getName(), ep.getSecond().getName());
            Futures.addCallback(resultSetFuture, new CandSessionsCallback(monthCandidates, query, ep.getFirst().getName(), ep.getSecond().getName(), start_date, end_date, doneSignal, ids_field_name), epThread);
        }

        for (Integer stepCount : allDetails.keySet()) {
            List<AugmentedDetail> stepDetails = allDetails.get(stepCount);
            for (AugmentedDetail det : stepDetails) // Query (async) for all the details of the funnel
            {
                ResultSetFuture resultSetFuture = session.executeAsync("SELECT " + ids_field_name + " FROM " + cassandra_keyspace_name + "." + tableName + " WHERE first_field = ? AND second_field = ? AND third_field = ?", det.getFirst(), det.getSecond(), det.getThird());
                Futures.addCallback(resultSetFuture, new CandSessionsCallback(monthCandidates, query, det.getFirst(), det.getSecond(), start_date, end_date, doneSignal, ids_field_name), detThread);
            }
        }

        try {
            doneSignal.await(); // Wait until all async queries have finished
        } catch (InterruptedException ex) {
            Logger.getLogger(SequenceQueryEvaluator.class.getName()).log(Level.SEVERE, null, ex);
        }

        epThread.shutdown();
        detThread.shutdown();

        return monthCandidates;
    }

    /**
     * For each candidate we check for two requirements: 1) The events of that candidate occur in the correct order,
     * 2) They occur within the timeframe of the "maxDuration" variable
     *
     * @param query       The query sequence
     * @param candidates  A list of call candidates
     * @param maxDuration The max duration timeframe
     * @return A set of all true positives along with their completion duration
     */
    public Map<String, Long> findTruePositives(Sequence query, Set<String> candidates, long maxDuration) {
        Map<String, Long> truePositives = new HashMap<String, Long>();
        long maxDate = 0;
        for (String candidate : candidates) {
            List<TimestampedEvent> allEvents = SequenceQueryHandler.allEventsPerSession.get(candidate);
            Collections.sort(allEvents);
            long[] timestamps = query.fitsTimestamps(allEvents, maxDuration);
            if (timestamps[0] == -1 && timestamps[1] == -1)
                continue;
            truePositives.put(candidate, timestamps[1] - timestamps[0]);
            if (timestamps[1] > maxDate)
                maxDate = timestamps[1];
        }
        truePositives.put("maxDate", maxDate);
        return truePositives;
    }

    /**
     * For each candidate we check for two requirements: 1) The events of that candidate occur in the correct order,
     * 2) They occur within the timeframe of the "maxDuration" variable
     *
     * @param query       The query sequence
     * @param candidates  A list of call candidates
     * @param maxDuration The max duration timeframe
     * @return A set of all true positives along with their Lifetime
     */
    public Map<String, Lifetime> findTruePositivesAndLifetime(Sequence query, Set<String> candidates, long maxDuration) {
        Map<String, Lifetime> truePositives = new HashMap<String, Lifetime>();
        long maxDate = 0;
        for (String candidate : candidates) {
            List<TimestampedEvent> allEvents = SequenceQueryHandler.allEventsPerSession.get(candidate);
            Collections.sort(allEvents);
            long[] timestamps = query.fitsTimestamps(allEvents, maxDuration);
            if (timestamps[0] == -1 && timestamps[1] == -1)
                continue;
            truePositives.put(candidate, new Lifetime(new Date(timestamps[0]), new Date(timestamps[1]), timestamps[1] - timestamps[0], ""));
            if (timestamps[1] > maxDate)
                maxDate = timestamps[1];
        }
        return truePositives;
    }

    /**
     * Given a set of user IDs return the corresponding device IDs
     *
     * @param start_date Funnel start date
     * @param end_date   Funnel end date
     * @param usrIDs     The set of user IDs
     * @return a set of device IDs
     */
    public Map<String, List<String>> getDeviceIDs(Date start_date, Date end_date, Set<String> usrIDs) {
        List<String> year_months = getYearMonths(start_date, end_date);

        Map<String, List<String>> dvcToUsrIDs = Collections.synchronizedMap(new HashMap<String, List<String>>());
        for (String year_month : year_months) {
            String tableName = "usr_device_" + year_month;
            if (ks.getTable(tableName) == null) // If the keyspace doesn't contain the corresponding table, move on to the next month
                continue;

            final ExecutorService devThread = Executors.newSingleThreadExecutor();

            final CountDownLatch doneSignal; // The countdown will reach zero once all threads have finished their task
            doneSignal = new CountDownLatch(Math.max(usrIDs.size(), 0));

            PreparedStatement statement = session.prepare("SELECT device FROM " + cassandra_keyspace_name + "." + tableName + " WHERE user = ?");

            for (String usr : usrIDs) {
                if (dvcToUsrIDs.containsKey(usr)) {
                    doneSignal.countDown();
                    continue;
                }

                ResultSetFuture resultSetFuture = session.executeAsync(statement.bind(usr));
                Futures.addCallback(resultSetFuture, new FutureCallback<ResultSet>() {
                    @Override
                    public void onSuccess(ResultSet result) {
                        Row row = result.one();

                        List<String> deviceID;
                        if (row != null) {
                            deviceID = row.getList("device", String.class);
                            dvcToUsrIDs.put(usr, deviceID);
                        }

                        doneSignal.countDown();
                    }

                    @Override
                    public void onFailure(Throwable thrwbl) {
                        System.out.println(thrwbl.getMessage());
                        System.out.println("UserID to DeviceID query failed!");
                        doneSignal.countDown();
                    }

                }, devThread);
            }

            try {
                doneSignal.await(); // Wait until all async queries have finished
            } catch (InterruptedException ex) {
                Logger.getLogger(SequenceQueryEvaluator.class.getName()).log(Level.SEVERE, null, ex);
            }

            devThread.shutdown();
        }

        return dvcToUsrIDs;
    }

    /**
     * Given a set of device IDs return the corresponding user IDs
     *
     * @param start_date Funnel start date
     * @param end_date   Funnel end date
     * @param dvcIDs     The set of device IDs
     * @return a set of user IDs
     */
    public Map<String, List<String>> getUserIDs(Date start_date, Date end_date, Set<String> dvcIDs) {
        List<String> year_months = getYearMonths(start_date, end_date);

        Map<String, List<String>> dvcToUserIDs = Collections.synchronizedMap(new HashMap<String, List<String>>());
        for (String year_month : year_months) {
            String tableName = "dvc_user_" + year_month;
            if (ks.getTable(tableName) == null) // If the keyspace doesn't contain the corresponding table, move on to the next month
                continue;

            final ExecutorService devThread = Executors.newSingleThreadExecutor();

            final CountDownLatch doneSignal; // The countdown will reach zero once all threads have finished their task
            doneSignal = new CountDownLatch(Math.max(dvcIDs.size(), 0));

            PreparedStatement statement = session.prepare("SELECT user FROM " + cassandra_keyspace_name + "." + tableName + " WHERE device = ?");

            for (String dvc : dvcIDs) {
                if (dvcToUserIDs.containsKey(dvc)) {
                    doneSignal.countDown();
                    continue;
                }

                ResultSetFuture resultSetFuture = session.executeAsync(statement.bind(dvc));
                Futures.addCallback(resultSetFuture, new FutureCallback<ResultSet>() {
                    @Override
                    public void onSuccess(ResultSet result) {
                        Row row = result.one();

                        List<String> userIDs;
                        if (row != null) {
                            userIDs = row.getList("user", String.class);
                            dvcToUserIDs.put(dvc, userIDs);
                        }

                        doneSignal.countDown();
                    }

                    @Override
                    public void onFailure(Throwable thrwbl) {
                        System.out.println("DeviceID to UserID query failed!");
                        doneSignal.countDown();
                    }

                }, devThread);
            }

            try {
                doneSignal.await(); // Wait until all async queries have finished
            } catch (InterruptedException ex) {
                Logger.getLogger(SequenceQueryEvaluator.class.getName()).log(Level.SEVERE, null, ex);
            }

            devThread.shutdown();
        }

        return dvcToUserIDs;
    }
}
