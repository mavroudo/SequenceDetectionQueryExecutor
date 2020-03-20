 package com.fa.funnel.rest.query;

import com.datastax.driver.core.*;
import com.fa.funnel.rest.model.Event;
import com.fa.funnel.rest.model.Sequence;
import com.fa.funnel.rest.model.TimestampedEvent;
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
import java.time.LocalDate;
import java.time.ZoneId;
/**
 * Class designed to handle the low-level needs of a query evaluation. Each time a query fetches data from the index (not count) tables the incoming rows are handled by this class first.
 * Each row is handled by extracting info about the entity (device or user) ID that achieved the completion and then handling the completion itself in one of two different ways (i.e. as an event pair or as a detail triplet)
 * @author Andreas Kosmatopoulos
 */
public class SequenceQueryHandler 
{

    /**
     * The cluster instance
     */
    protected Cluster cluster = null;

    /**
     * The session instance
     */
    protected Session session = null;

    /**
     * The keyspace metadata instance
     */
    protected KeyspaceMetadata ks = null;

    /**
     * The keyspace name
     */
    protected String cassandra_keyspace_name = null;
    
    /**
     * The delimiter used in the contents of a row
     */
    protected static final String DELAB_DELIMITER = "¦delab¦";
    
    /**
     * A ConcurrentHashMap that contains for all entities (device or user) solution candidates the events that this entity has performed
     */
    protected static ConcurrentHashMap<String, List<TimestampedEvent>> allEventsPerSession;
    
    /**
     * Constructor
     * @param cluster The cluster instance
     * @param session The session instance
     * @param ks The keyspace metadata instance
     * @param cassandra_keyspace_name The keyspace name
     */
    public SequenceQueryHandler (Cluster cluster, Session session, KeyspaceMetadata ks, String cassandra_keyspace_name)
    {
        this.cluster = cluster;
        this.session = session;
        this.ks = ks;
        this.cassandra_keyspace_name = cassandra_keyspace_name;
    }
    
    /**
     * Strip the appID and logtypeID from an event name 
     * @param event The event
     * @return the event name without appID and logtypeID
     */
    public static String stripAppIDAndLogtype(String event)
    {
        return event.replaceFirst("(\\d)+_", "").replaceFirst("(\\d)+_", "");
    }

    /**
     * Returns a list of all months between the two provided dates (each month is also divided in three 10-day intervals)
     * @param start_date Funnel start date
     * @param end_date Funnel end date
     * @return a list of all year_months_10dayinterval (i.e. 2018_02_2, 2018_02_3, 2018_03_1, ... etc.) between the two provided dates
     */
    public static List<String> getYearMonths(Date start_date, Date end_date) 
    {
        TreeSet<String> year_months = new TreeSet<String>();

        LocalDate start = start_date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        LocalDate end = end_date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        
        for (LocalDate date = start; date.isBefore(end); date = date.plusDays(1))
        {
            int day = date.getDayOfMonth();
            int period;
            if (day <= 10)
                period = 1;
            else if (day <= 20)
                period = 2;
            else
                period = 3;
            year_months.add(date.getYear() + "_" + String.format("%02d", date.getMonthValue()) + "_" + period);
        }

        return new ArrayList<String>(year_months);
    }
    
    /**
     * Retrieve each candidate ID and then handle each row differently depending on if it corresponds to event pairs or detail pairs
     * @param query The query sequence
     * @param first The first event
     * @param second The second event
     * @param start_date Funnel start date
     * @param end_date Funnel end date
     * @param candSessions A list of all candidates (i.e. rows with timestamps)
     * @param putInMap If this is true, put any timestamps in the "allEventsPerSession" map 
     * @return a list of all candidate entity IDs
     */
    protected static List<String> handleSessionsRow(Sequence query, String first, String second, Date start_date, Date end_date, List<String> candSessions, boolean putInMap)
    {
        ArrayList<String> newlist = new ArrayList<String>();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//        Pattern pattern = Pattern.compile("([a-zA-Z0-9\\-\\@\\.]*)" + DELAB_DELIMITER + "\\((.*)\\)");
        Pattern pattern = Pattern.compile("(.*?)" + DELAB_DELIMITER + "\\((.*)\\)");

        for (String sessionInfo : candSessions) 
        {
            sessionInfo = sessionInfo.replaceAll("\\[", "").replaceAll("\\]", "");
            Matcher matcher = pattern.matcher(sessionInfo);
            matcher.matches();
            String sessionID = matcher.group(1);
            newlist.add(sessionID);
            
            if (putInMap && !allEventsPerSession.containsKey(sessionID))
                allEventsPerSession.put(sessionID, Collections.synchronizedList(new ArrayList<TimestampedEvent>()));
            
            if (sessionInfo.endsWith(DELAB_DELIMITER + "()")) // This is an empty sessions row (should be removed by the filter)
                continue;
            if (sessionInfo.contains("((")) // Then this is an Event pair
            {
                String[] times = matcher.group(2).split("\\), \\(");
                handleEventPairs(dateFormat, sessionID, times, first, second, start_date, end_date);
            }
            else // Then this a detail pair
            {
                String event_key_value = first + "_" + second;
                String[] times = matcher.group(2).split(", ");
                handleDetailPairs(dateFormat, sessionID, times, event_key_value, start_date, end_date);
            }
        }
        return newlist;
    }
    
    private static void handleEventPairs(SimpleDateFormat dateFormat, String sessionID, String[] times, String first, String second, Date start_date, Date end_date) 
    {
        for (String time : times) 
        {
            time = time.replaceAll("\\(", "").replaceAll("\\)", "");
            String time_first_str = time.split(",")[0];
            String time_second_str = time.split(",")[1];
            
            Date time_first = null;
            Date time_second = null;

            try 
            {
                time_first = dateFormat.parse(time_first_str.substring(0, time_first_str.length() - 3));
                time_second = dateFormat.parse(time_second_str.substring(0, time_second_str.length() - 3));
            } 
            catch (ParseException ex) 
            {
                Logger.getLogger(SequenceQueryEvaluator.class.getName()).log(Level.SEVERE, null, ex);
            }

            if (allEventsPerSession.containsKey(sessionID)) 
            {
                if (time_first.after(start_date) && time_first.before(end_date))
                    allEventsPerSession.get(sessionID).add(new TimestampedEvent(time_first, new Event(first)));
                if (time_second.after(start_date) && time_second.before(end_date))
                    allEventsPerSession.get(sessionID).add(new TimestampedEvent(time_second, new Event(second)));
            }
        }
    }
    
    private static void handleDetailPairs(SimpleDateFormat dateFormat, String sessionID, String[] times, String event_key_value, Date start_date, Date end_date) 
    {
        for (String time : times) 
        {
            if (allEventsPerSession.containsKey(sessionID)) 
            {
                try 
                {
                    Date detail_time = dateFormat.parse(time.substring(0, time.length() - 3));
                    if (detail_time.after(start_date) && detail_time.before(end_date))
                        allEventsPerSession.get(sessionID).add(new TimestampedEvent(detail_time, new Event(event_key_value)));
                } 
                catch (ParseException ex) 
                {
                    Logger.getLogger(SequenceQueryHandler.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }
    
    protected static class CandSessionsCallback implements FutureCallback<ResultSet>
    {
        Collection<String> monthCandidates;
        Sequence query;
        String first;
        String second;
        String third;
        Date start_date;
        Date end_date;
        CountDownLatch doneSignal;
        String ids_field_name;

        public CandSessionsCallback(Collection<String> monthCandidates, Sequence query, String first, String second, Date start_date, Date end_date, CountDownLatch doneSignal, String ids_field_name)
        {
            this.monthCandidates = monthCandidates;
            this.query = query;
            this.first = first;
            this.second = second;
            this.start_date = start_date;
            this.end_date = end_date;
            this.doneSignal = doneSignal;
            this.ids_field_name = ids_field_name;
        }

        @Override
        public void onSuccess(ResultSet result) 
        {
            Row row = result.one();

            List<String> candSessions;
            if (row != null) 
            {
                candSessions = row.getList(ids_field_name, String.class);
                candSessions = handleSessionsRow(query, first, second, start_date, end_date, candSessions, false);
            } 
            else 
                candSessions = new ArrayList<String>();
            
            monthCandidates.retainAll(new HashSet<String>(candSessions));

            doneSignal.countDown();
        }

        @Override
        public void onFailure(Throwable t) 
        {
            System.out.println("Cassandra query failed! - " + first + "/" + second + "/" + third);
            System.out.println(t.getMessage());
            doneSignal.countDown();
        }
    }
}
