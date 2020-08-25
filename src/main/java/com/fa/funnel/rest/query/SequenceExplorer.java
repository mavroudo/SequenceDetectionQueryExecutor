package com.fa.funnel.rest.query;

import com.datastax.driver.core.*;
import com.fa.funnel.rest.model.*;

import java.util.*;

/**
 * The class responsible for performing funnel exploration. In the case of accurate exploration each continuation is evaluated precisely using the
 * index tables while in the case of an fast exploration the class makes use of the count tables.
 * This class is similar to SequenceQueryExplorer made for FollowApps but for the log file exploration
 */
public class SequenceExplorer extends SequenceQueryEvaluator {

    /**
     * Constructor
     *
     * @param cluster                 The cluster instance
     * @param session                 The session instance
     * @param ks                      The keyspace metadata instance
     * @param cassandra_keyspace_name The keyspace name
     */
    public SequenceExplorer(Cluster cluster, Session session, KeyspaceMetadata ks, String cassandra_keyspace_name) {
        super(cluster, session, ks, cassandra_keyspace_name);
    }

    public Map<String, Map<String, Long>> exploreQueryAccurateStep(Date start_date, Date end_date, Sequence query, Map<Integer, List<AugmentedDetail>> details, String table_name, int position, long maxDuration) {
        List<EventPairFrequency> freqs;

        if (position > 0 && position < query.getSize()) // Intermediate explore
            freqs = getAllPossibleIntermediateEvents(query.getEvent(position - 1), table_name);
        else if (position == query.getSize()) // Forward Explore
            freqs = getEventFrequencyCounts(query.getLastEvent().toString(), table_name);
        else if (position == 0) // Prefix Explore
            freqs = getAllPossiblePrecedingEvents(query.getEvent(0), table_name);
        else
            freqs = null;

        Map<String, Map<String, Long>> allTPsPerEvent = new HashMap<>();

        if (freqs != null) {
            for (EventPairFrequency freq : freqs) {
                String event = freq.getEventPair().getSecond();
                Sequence tempquery = new Sequence(query);
                if (position > 0 && position < query.getSize())
                    tempquery.insert(new Event(event), position);
                else if (position == query.getSize())
                    tempquery.appendToSequence(new Event(event));
                else if (position == 0)
                    tempquery.prependToSequence(new Event(event));
                Set<String> candidates = evaluateQueryLogFile(start_date, end_date, tempquery, details, table_name + "_idx");

                Map<String, Long> truePositives = findTruePositives(tempquery, candidates, maxDuration);

                allTPsPerEvent.put(event, truePositives);
            }
        }

        return allTPsPerEvent;
    }

    private List<EventPairFrequency> getAllPossiblePrecedingEvents(Event second, String table_name) {
        HashSet<String> allEvents = new HashSet<>();
        String tableName = table_name + "_idx";
        if (ks.getTable(tableName) == null) // If the keyspace doesn't contain the corresponding table, move on to the next month
            return new ArrayList<>();

        // Perform a non-async query to retrieve the event names
        // WARNING: Use of allow filtering
        ResultSet rs = session.execute("SELECT event1_name FROM " + cassandra_keyspace_name + "." + tableName + " WHERE event2_name = ?  ALLOW FILTERING;", second.getName());

        List<Row> rows = rs.all();

        for (Row row : rows) {
            if (row == null)
                continue;
            String eventName = row.getString("event1_name");
            allEvents.add(eventName);
        }

        ArrayList<EventPairFrequency> result = new ArrayList<>();
        for (String event : allEvents)
            result.add(new EventPairFrequency(new EventPair("n/a", event), 0.0, 0.0));

        return result;
    }

    private List<EventPairFrequency> getEventFrequencyCounts(String queryEvent, String table_name) {
        HashMap<String, Double> allDurationSums = new HashMap<>();
        HashMap<String, Double> allCompCounts = new HashMap<>();

        String tableName = table_name + "_count";

        if (ks.getTable(tableName) == null) // If the keyspace doesn't contain the corresponding table, move on to the next month
            return new ArrayList<>();

        // Perform a non-async query to retrieve the counts of that event
        ResultSet rs = session.execute("SELECT sequences_per_field FROM " + cassandra_keyspace_name + "." + tableName + " WHERE event1_name = ?", queryEvent);

        Row row = rs.one();

        List<String> monthFrequencies;
        if (row != null)
            monthFrequencies = row.getList("sequences_per_field", String.class);
        else
            monthFrequencies = new ArrayList<>();

        for (String epFreq : monthFrequencies) {
            String event = epFreq.split(DELAB_DELIMITER)[0];
            Double sum_duration = Double.parseDouble(epFreq.split(DELAB_DELIMITER)[1]);
            Double comp_count = Double.parseDouble(epFreq.split(DELAB_DELIMITER)[2]);
            if (allCompCounts.containsKey(event)) {
                allDurationSums.put(event, allDurationSums.get(event) + sum_duration);
                allCompCounts.put(event, allCompCounts.get(event) + comp_count);
            } else {
                allDurationSums.put(event, sum_duration);
                allCompCounts.put(event, comp_count);
            }
        }


        ArrayList<EventPairFrequency> result = new ArrayList<>();
        for (String event : allCompCounts.keySet())
            result.add(new EventPairFrequency(new EventPair(queryEvent, event), allDurationSums.get(event), allCompCounts.get(event)));

        result.sort(Collections.reverseOrder());
        return result;
    }

    private List<EventPairFrequency> getAllPossibleIntermediateEvents(Event first, String table_name) {
        HashSet<String> allEvents = new HashSet<>();
        String tableName = table_name + "_idx";
        if (ks.getTable(tableName) == null) // If the keyspace doesn't contain the corresponding table, move on to the next month
            return new ArrayList<>();


        // Perform a non-async query to retrieve the event names
        // WARNING: Use of allow filtering
        ResultSet rs = session.execute("SELECT event2_name FROM " + cassandra_keyspace_name + "." + tableName + " WHERE event1_name = ? ALLOW FILTERING;", first.getName());

        List<Row> rows = rs.all();

        for (Row row : rows) {
            if (row == null)
                continue;
            String eventName = row.getString("event2_name");
            allEvents.add(eventName);
        }

        ArrayList<EventPairFrequency> result = new ArrayList<>();
        for (String event : allEvents)
            result.add(new EventPairFrequency(new EventPair("n/a", event), 0.0, 0.0));
        return result;
    }

    /**
     * Get the completion count of a specified QueryPair
     *
     * @param start_date (Sub)-funnel start date
     * @param end_date   (Sub)-funnel end date
     * @param qp         The specified QueryPair
     * @return The completion count for this QueryPair
     */
    public int getCountForQueryPair(Date start_date, Date end_date, QueryPair qp, String table_name) {
        List<EventPairFrequency> freqs = getEventFrequencyCounts(qp.getFirst().toString(), table_name);

        for (EventPairFrequency freq : freqs)
            if (freq.getEventPair().getSecond().equals(qp.getSecond().toString()))
                return freq.getCompCount().intValue();

        return 0;
    }

    /**
     * Performs a fast evaluation of each possible continuation. Used in the {@code /export/fast} and {@code /export/hybrid} endpoints
     *
     * @param start_date      Funnel start date
     * @param end_date        Funnel end date
     * @param query           The query sequence
     * @param details         The details accompanying the query
     * @param position        Position index on the query sequence where continuations are to be explored
     * @param lastCompletions An estimation of the completions of the original query sequence. Due to the Apriori principle any continuation is upper bounded by this numbers
     * @return a set of true positives (along with their completion duration) for each possible continuation
     */
    public List<Proposition> exploreQueryFastStep(Date start_date, Date end_date, Sequence query, Map<Integer, List<AugmentedDetail>> details, String table_name, int position, int lastCompletions) {
        List<Proposition> props = new ArrayList<Proposition>();

        int max = lastCompletions;

        if (position > 0 && position < query.getSize()) // Intermediate explore
        {
            String lastEvents = query.getEvent(position - 1).toString();
            List<EventPairFrequency> freqs = getEventFrequencyCounts(lastEvents, table_name);

//            ENHANCEMENT: Use reverse count indices to provide better result: e.g. on A->B->_blank_->D->E get EventPairFrequency for A->B and reverse EventPairFrequency for D
//            String firstEvents = query.getEvent(position+1).toString();
//            List<EventPairFrequency> rev_freqs = getReverseEventFrequencyCounts(application_id, log_type, year_months, firstEvents, isUsersQuery);

            for (EventPairFrequency freq : freqs) {
                int upper = Math.min(max, freq.getCompCount().intValue());
                Proposition prop = new Proposition(0, 0, freq.getEventPair().getSecond(), upper, (freq.getAvgDuration() / 1000.0));

                if (upper != 0) // Only show interesting results
                    props.add(prop);
            }

            //CONTINUE ENHANCEMENT FROM ABOVE
        } else if (position >= query.getSize()) // Forward explore
        {
            List<EventPairFrequency> freqs = getEventFrequencyCounts(query.getLastEvent().toString(), table_name);

            for (EventPairFrequency freq : freqs) {
                int upper = Math.min(max, freq.getCompCount().intValue());
                Proposition prop = new Proposition(0, 0, freq.getEventPair().getSecond(), upper, (freq.getAvgDuration() / 1000.0));

                if (upper != 0) // Only show interesting results
                    props.add(prop);
            }
        } else // Prefix explore
        {
            List<EventPairFrequency> rev_freqs = getReverseEventFrequencyCounts(query.getFirstEvent().toString(), table_name);

            for (EventPairFrequency freq : rev_freqs) {
                int upper = Math.min(max, freq.getCompCount().intValue());
                Proposition prop = new Proposition(0, 0, freq.getEventPair().getSecond(), upper, (freq.getAvgDuration() / 1000.0));

                if (upper != 0) // Only show interesting results
                    props.add(prop);
            }
        }

        props.sort(Collections.reverseOrder());
        return props;
    }

    private List<EventPairFrequency> getReverseEventFrequencyCounts(String queryEvent, String table_name) {
        HashMap<String, Double> allDurationSums = new HashMap<String, Double>();
        HashMap<String, Double> allCompCounts = new HashMap<String, Double>();
        String tableName = table_name + "_rvc_count";


        String ids_field_name = "sequences_per_field";


        if (ks.getTable(tableName) == null) // If the keyspace doesn't contain the corresponding table, move on to the next month
            return new ArrayList<>();

        // Perform a non-async query to retrieve the counts of that event
        ResultSet rs = session.execute("SELECT " + ids_field_name + " FROM " + cassandra_keyspace_name + "." + tableName + " WHERE first_field = ?", queryEvent);

        Row row = rs.one();

        List<String> monthFrequencies;
        if (row != null)
            monthFrequencies = row.getList(ids_field_name, String.class);
        else
            monthFrequencies = new ArrayList<String>();

        for (String epFreq : monthFrequencies) {
            if (epFreq.split(DELAB_DELIMITER).length > 3)
                continue;

            String event = epFreq.split(DELAB_DELIMITER)[0];
            Double sum_duration = Double.parseDouble(epFreq.split(DELAB_DELIMITER)[1]);
            Double comp_count = Double.parseDouble(epFreq.split(DELAB_DELIMITER)[2]);
            if (allCompCounts.containsKey(event)) {
                allDurationSums.put(event, allDurationSums.get(event) + sum_duration);
                allCompCounts.put(event, allCompCounts.get(event) + comp_count);
            } else {
                allDurationSums.put(event, sum_duration);
                allCompCounts.put(event, comp_count);
            }
        }


        ArrayList<EventPairFrequency> result = new ArrayList<EventPairFrequency>();
        for (String event : allCompCounts.keySet())
            result.add(new EventPairFrequency(new EventPair(queryEvent, event), allDurationSums.get(event), allCompCounts.get(event)));

        Collections.sort(result, Collections.reverseOrder());
        return result;
    }


}
