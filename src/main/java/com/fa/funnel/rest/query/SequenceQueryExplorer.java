package com.fa.funnel.rest.query;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.fa.funnel.rest.model.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The class responsible for performing funnel exploration. In the case of accurate exploration each continuation is evaluated precisely using the
 * index tables while in the case of an fast exploration the class makes use of the count tables
 * @author Andreas Kosmatopoulos
 */
public class SequenceQueryExplorer extends SequenceQueryEvaluator
{

    /**
     * Constructor
     * @param cluster The cluster instance
     * @param session The session instance
     * @param ks The keyspace metadata instance
     * @param cassandra_keyspace_name The keyspace name
     */
    public SequenceQueryExplorer(Cluster cluster, Session session, KeyspaceMetadata ks, String cassandra_keyspace_name)
    {
        super(cluster, session, ks, cassandra_keyspace_name);
    }
    
    /**
     * Performs an accurate evaluation of each possible continuation. Used in the {@code /export/accurate} endpoint
     * @param start_date Funnel start date
     * @param end_date Funnel end date
     * @param query The query sequence
     * @param details The details accompanying the query
     * @param position Position index on the query sequence where continuations are to be explored
     * @param targetAppID If set, restrict results only to the particular appID
     * @param maxDuration The max duration timeframe
     * @param isUsersQuery Boolean representing if the query is cross-app or not
     * @return a set of true positives (along with their completion duration) for each possible continuation
     */
    public Map<String, Map<String, Long>> exploreQueryAccurateStep(Date start_date, Date end_date, Sequence query, Map<Integer, List<AugmentedDetail>> details, int position, String targetAppID, long maxDuration, boolean isUsersQuery)
    {
        List<String> year_months = getYearMonths(start_date, end_date); // Returns all months and years between "start_date" and "end_date", e.g. "2017_06", "2017_07", ..., "2017_10"
        List<EventPairFrequency> freqs;

        if (position > 0 && position < query.getSize()) // Intermediate explore
            freqs = getAllPossibleIntermediateEvents(year_months, query.getEvent(position-1), isUsersQuery);
        else if (position == query.getSize()) // Forward Explore
            freqs = getEventFrequencyCounts(year_months, query.getLastEvent().toString(), isUsersQuery);
        else if (position == 0) // Prefix Explore
            freqs = getAllPossiblePrecedingEvents(year_months, query.getEvent(0), isUsersQuery);
        else
            freqs = null;

        Map<String, Map<String, Long>> allTPsPerEvent = new HashMap<String, Map<String, Long>>();
        
        for (int j = 0; j < freqs.size(); j++)
        {
            String event = freqs.get(j).getEventPair().getSecond();
            if (!targetAppID.isEmpty())
                if (!event.startsWith(targetAppID))
                    continue;
                
            Sequence tempquery = new Sequence(query);
            if (position > 0 && position < query.getSize())
                tempquery.insert(new Event(event), position);
            else if (position == query.getSize())
                tempquery.appendToSequence(new Event(event));
            else if (position == 0)
                tempquery.prependToSequence(new Event(event));
            Set<String> candidates = evaluateQuery(start_date, end_date, tempquery, details, isUsersQuery);

            Map<String, Long> truePositives = findTruePositives(tempquery, candidates, maxDuration);

            allTPsPerEvent.put(event, truePositives);
        }

        return allTPsPerEvent;
    }

    /**
     * Performs a fast evaluation of each possible continuation. Used in the {@code /export/fast} and {@code /export/hybrid} endpoints
     * @param start_date Funnel start date
     * @param end_date Funnel end date
     * @param query The query sequence
     * @param details The details accompanying the query
     * @param position Position index on the query sequence where continuations are to be explored
     * @param targetAppID If set, restrict results only to the particular appID
     * @param lastCompletions An estimation of the completions of the original query sequence. Due to the Apriori principle any continuation is upper bounded by this number
     * @param isUsersQuery Boolean representing if the query is cross-app or not
     * @return a set of true positives (along with their completion duration) for each possible continuation
     */
    public List<Proposition> exploreQueryFastStep(Date start_date, Date end_date, Sequence query, Map<Integer, List<AugmentedDetail>> details, int position, String targetAppID, int lastCompletions, boolean isUsersQuery)
    {
        List<Proposition> props = new ArrayList<Proposition>();

        List<String> year_months = getYearMonths(start_date, end_date); // Returns all months and years between "start_date" and "end_date", e.g. "2017_06", "2017_07", ..., "2017_10"

        int max = lastCompletions;

        if (position > 0 && position < query.getSize()) // Intermediate explore
        {
            String lastEvents = query.getEvent(position-1).toString();
            List<EventPairFrequency> freqs = getEventFrequencyCounts(year_months, lastEvents, isUsersQuery);    

//            ENHANCEMENT: Use reverse count indices to provide better result: e.g. on A->B->_blank_->D->E get EventPairFrequency for A->B and reverse EventPairFrequency for D            
//            String firstEvents = query.getEvent(position+1).toString();
//            List<EventPairFrequency> rev_freqs = getReverseEventFrequencyCounts(application_id, log_type, year_months, firstEvents, isUsersQuery);

            for (EventPairFrequency freq : freqs) 
            {
                if (!targetAppID.isEmpty())
                    if (!freq.getEventPair().getSecond().startsWith(targetAppID))
                        continue;
                int upper = (max < freq.getCompCount().intValue()) ? max : freq.getCompCount().intValue();
                Proposition prop = new Proposition(Integer.parseInt(freq.getEventPair().getSecond().split("_")[0]), Integer.parseInt(freq.getEventPair().getSecond().split("_")[1]), stripAppIDAndLogtype(freq.getEventPair().getSecond()), upper, (freq.getAvgDuration() / 1000.0));

                if (upper != 0) // Only show interesting results
                    props.add(prop);
            }
            
            //CONTINUE ENHANCEMENT FROM ABOVE
        }
        else if (position >= query.getSize()) // Forward explore
        {
            List<EventPairFrequency> freqs = getEventFrequencyCounts(year_months, query.getLastEvent().toString(), isUsersQuery);

            for (EventPairFrequency freq : freqs)
            {
                if (!targetAppID.isEmpty())
                    if (!freq.getEventPair().getSecond().startsWith(targetAppID))
                        continue;
                int upper = (max < freq.getCompCount().intValue() ) ? max : freq.getCompCount().intValue();
                Proposition prop = new Proposition(Integer.parseInt(freq.getEventPair().getSecond().split("_")[0]), Integer.parseInt(freq.getEventPair().getSecond().split("_")[1]), stripAppIDAndLogtype(freq.getEventPair().getSecond()), upper, (freq.getAvgDuration() / 1000.0));

                if (upper != 0) // Only show interesting results
                    props.add(prop);
            }
        }
        else // Prefix explore
        {
            List<EventPairFrequency> rev_freqs = getReverseEventFrequencyCounts(year_months, query.getFirstEvent().toString(), isUsersQuery);
            
            for (EventPairFrequency freq : rev_freqs)
            {
                if (!targetAppID.isEmpty())
                    if (!freq.getEventPair().getSecond().startsWith(targetAppID))
                        continue;
                int upper = (max < freq.getCompCount().intValue() ) ? max : freq.getCompCount().intValue();
                Proposition prop = new Proposition(Integer.parseInt(freq.getEventPair().getSecond().split("_")[0]), Integer.parseInt(freq.getEventPair().getSecond().split("_")[1]), stripAppIDAndLogtype(freq.getEventPair().getSecond()), upper, (freq.getAvgDuration() / 1000.0));

                if (upper != 0) // Only show interesting results
                    props.add(prop);
            }
        }

        Collections.sort(props, Collections.reverseOrder());
        return props;
    }
    
    private List<EventPairFrequency> getEventFrequencyCounts(List<String> year_months, String queryEvent, boolean isUsersQuery) 
    {
        HashMap<String, Double> allDurationSums = new HashMap<String, Double>();
        HashMap<String, Double> allCompCounts = new HashMap<String, Double>();

        String ids_field_name = (!isUsersQuery ? "devices_per_field" : "users_per_field");

        for (String year_month : year_months) 
        {
            String tableName = (!isUsersQuery ? "dvc_count_" + year_month : "usr_count_" + year_month);
            if (ks.getTable(tableName) == null) // If the keyspace doesn't contain the corresponding table, move on to the next month
                continue;

            // Perform a non-async query to retrieve the counts of that event
            ResultSet rs = session.execute("SELECT " + ids_field_name + " FROM " + cassandra_keyspace_name + "." + tableName + " WHERE first_field = ?", queryEvent);

            Row row = rs.one();

            List<String> monthFrequencies;
            if (row != null)
                monthFrequencies = row.getList(ids_field_name, String.class);
            else
                monthFrequencies = new ArrayList<String>();

            for (String epFreq : monthFrequencies) 
            {
                String event = epFreq.split(DELAB_DELIMITER)[0];
                Double sum_duration = Double.parseDouble(epFreq.split(DELAB_DELIMITER)[1]);
                Double comp_count = Double.parseDouble(epFreq.split(DELAB_DELIMITER)[2]);
                if (allCompCounts.containsKey(event))
                {
                    allDurationSums.put(event, allDurationSums.get(event) + sum_duration);
                    allCompCounts.put(event, allCompCounts.get(event) + comp_count);
                }
                else
                {
                    allDurationSums.put(event, sum_duration);
                    allCompCounts.put(event, comp_count);
                }
            }
        }

        ArrayList<EventPairFrequency> result = new ArrayList<EventPairFrequency>();
        for (String event : allCompCounts.keySet())
            result.add(new EventPairFrequency(new EventPair(queryEvent, event), allDurationSums.get(event), allCompCounts.get(event)));

        Collections.sort(result, Collections.reverseOrder());
        return result;
    }
    
    private List<EventPairFrequency> getDetailFrequencyCounts(List<String> year_months, String queryEvent, String queryKey, boolean isUsersQuery) 
    {
        HashMap<String, Double> allDurationSums = new HashMap<String, Double>();
        HashMap<String, Double> allCompCounts = new HashMap<String, Double>();

        String ids_field_name = (!isUsersQuery ? "devices_per_field" : "users_per_field");

        for (String year_month : year_months) 
        {
            String tableName = (!isUsersQuery ? "dvc_count_" + year_month : "usr_count_" + year_month);
            if (ks.getTable(tableName) == null) // If the keyspace doesn't contain the corresponding table, move on to the next month
                continue;

            // Perform a non-async query to retrieve the counts of that event
            ResultSet rs = session.execute("SELECT " + ids_field_name + " FROM " + cassandra_keyspace_name + "." + tableName + " WHERE first_field = ?", queryEvent+DELAB_DELIMITER+queryKey);

            Row row = rs.one();

            List<String> monthFrequencies;
            if (row != null)
                monthFrequencies = row.getList(ids_field_name, String.class);
            else
                monthFrequencies = new ArrayList<String>();

            for (String epFreq : monthFrequencies) 
            {
                String value = epFreq.split(DELAB_DELIMITER)[0];
                Double sum_duration = Double.parseDouble(epFreq.split(DELAB_DELIMITER)[1]);
                Double comp_count = Double.parseDouble(epFreq.split(DELAB_DELIMITER)[2]);
                if (allCompCounts.containsKey(value))
                {
                    allDurationSums.put(value, allDurationSums.get(value) + sum_duration);
                    allCompCounts.put(value, allCompCounts.get(value) + comp_count);
                }
                else
                {
                    allDurationSums.put(value, sum_duration);
                    allCompCounts.put(value, comp_count);
                }
            }
        }

        ArrayList<EventPairFrequency> result = new ArrayList<EventPairFrequency>();
        for (String value : allCompCounts.keySet())
            result.add(new EventPairFrequency(new EventPair(queryEvent+DELAB_DELIMITER+queryKey, value), allDurationSums.get(value), allCompCounts.get(value)));

        Collections.sort(result, Collections.reverseOrder());
        return result;
    }
    
    /**
     * Get the completion count of a specified QueryPair
     * @param start_date (Sub)-funnel start date
     * @param end_date (Sub)-funnel end date
     * @param qp The specified QueryPair
     * @param isUsersQuery Boolean representing if the query is cross-app or not
     * @return The completion count for this QueryPair
     */
    public int getCountForQueryPair(Date start_date, Date end_date, QueryPair qp, boolean isUsersQuery)
    {
        List<String> year_months = getYearMonths(start_date, end_date);
        List<EventPairFrequency> freqs = getEventFrequencyCounts(year_months, qp.getFirst().toString(), isUsersQuery);
        
        for (EventPairFrequency freq : freqs)
            if (freq.getEventPair().getSecond().equals(qp.getSecond().toString()))
                return freq.getCompCount().intValue();
        
        return 0;
    }
    
    /**
     * Get the completion count of a specified (Event,Key,Value) detail triplet
     * @param start_date (Sub)-funnel start date
     * @param end_date (Sub)-funnel end date
     * @param event The event name
     * @param key The detail key
     * @param value The detail value
     * @param isUsersQuery Boolean representing if the query is cross-app or not
     * @return The completion count for this (Event,Key,Value) detail triplet
     */
    public int getCountForEventKeyValueTriplet(Date start_date, Date end_date, String event, String key, String value, boolean isUsersQuery)
    {
        List<String> year_months = getYearMonths(start_date, end_date);
        List<EventPairFrequency> freqs = getDetailFrequencyCounts(year_months, event, key, isUsersQuery);
        
        for (EventPairFrequency freq : freqs)
            if (freq.getEventPair().getSecond().equals(value))
                return freq.getCompCount().intValue();
        
        return 0;
    }

    private List<EventPairFrequency> getAllPossiblePrecedingEvents(List<String> year_months, Event second, boolean isUsersQuery)
    {                                                                                                                                                             
        HashSet<String> allEvents = new HashSet<String>();
        for (String year_month : year_months)
        {
            String tableName = (!isUsersQuery ? "dvc_idx_" + year_month : "usr_idx_" + year_month);
            if (ks.getTable(tableName) == null) // If the keyspace doesn't contain the corresponding table, move on to the next month
                continue;
            
            // Perform a non-async query to retrieve the event names
            // WARNING: Use of allow filtering
            ResultSet rs = session.execute("SELECT first_field FROM " + cassandra_keyspace_name + "." + tableName + " WHERE second_field = ? AND third_field = 'null' ALLOW FILTERING;", second);

            List<Row> rows = rs.all();

            for (Row row : rows)
            {
                if (row == null)
                    continue; 
                String eventName = row.getString("first_field");
                allEvents.add(eventName);
            }
        }
        ArrayList<EventPairFrequency> result = new ArrayList<EventPairFrequency>();
        for (String event : allEvents)
            result.add(new EventPairFrequency(new EventPair("n/a",event),0.0,0.0));

        return result;
    }
    
    private List<EventPairFrequency> getAllPossibleIntermediateEvents(List<String> year_months, Event first, boolean isUsersQuery) 
    {
        HashSet<String> allEvents = new HashSet<String>();
        for (String year_month : year_months)
        {
            String tableName = (!isUsersQuery ? "dvc_idx_" + year_month : "usr_idx_" + year_month);
            if (ks.getTable(tableName) == null) // If the keyspace doesn't contain the corresponding table, move on to the next month
                continue;
            
            // Perform a non-async query to retrieve the event names
            // WARNING: Use of allow filtering
            ResultSet rs = session.execute("SELECT second_field FROM " + cassandra_keyspace_name + "." + tableName + " WHERE first_field = ? AND third_field = 'null' ALLOW FILTERING;", first);

            List<Row> rows = rs.all();

            for (Row row : rows)
            {
                if (row == null)
                    continue;
                String eventName = row.getString("second_field");
                allEvents.add(eventName);
            }
        }
        ArrayList<EventPairFrequency> result = new ArrayList<EventPairFrequency>();
        for (String event : allEvents)
            result.add(new EventPairFrequency(new EventPair("n/a",event),0.0,0.0));

//        Collections.sort(result, Collections.reverseOrder());
        return result;
    }

    private List<EventPairFrequency> getReverseEventFrequencyCounts(List<String> year_months, String queryEvent, boolean isUsersQuery) 
    {
        HashMap<String, Double> allDurationSums = new HashMap<String, Double>();
        HashMap<String, Double> allCompCounts = new HashMap<String, Double>();

        String ids_field_name = (!isUsersQuery ? "devices_per_field" : "users_per_field");
        //String ids_field_name = "users_per_field";

        for (String year_month : year_months) 
        {
            String tableName = (!isUsersQuery ? "reverse_dvc_count_" + year_month : "reverse_usr_count_" + year_month);
            if (ks.getTable(tableName) == null) // If the keyspace doesn't contain the corresponding table, move on to the next month
                continue;

            // Perform a non-async query to retrieve the counts of that event
            ResultSet rs = session.execute("SELECT " + ids_field_name + " FROM " + cassandra_keyspace_name + "." + tableName + " WHERE first_field = ?", queryEvent);

            Row row = rs.one();

            List<String> monthFrequencies;
            if (row != null)
                monthFrequencies = row.getList(ids_field_name, String.class);
            else
                monthFrequencies = new ArrayList<String>();

            for (String epFreq : monthFrequencies) 
            {
                if (epFreq.split(DELAB_DELIMITER).length > 3)
                    continue;
                
                String event = epFreq.split(DELAB_DELIMITER)[0];
                Double sum_duration = Double.parseDouble(epFreq.split(DELAB_DELIMITER)[1]);
                Double comp_count = Double.parseDouble(epFreq.split(DELAB_DELIMITER)[2]);
                if (allCompCounts.containsKey(event))
                {
                    allDurationSums.put(event, allDurationSums.get(event) + sum_duration);
                    allCompCounts.put(event, allCompCounts.get(event) + comp_count);
                }
                else
                {
                    allDurationSums.put(event, sum_duration);
                    allCompCounts.put(event, comp_count);
                }
            }
        }

        ArrayList<EventPairFrequency> result = new ArrayList<EventPairFrequency>();
        for (String event : allCompCounts.keySet())
            result.add(new EventPairFrequency(new EventPair(queryEvent, event), allDurationSums.get(event), allCompCounts.get(event)));

        Collections.sort(result, Collections.reverseOrder());
        return result;
    }
}

