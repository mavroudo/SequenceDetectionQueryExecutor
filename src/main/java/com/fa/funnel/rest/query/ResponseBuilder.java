package com.fa.funnel.rest.query;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.fa.funnel.rest.model.*;
import com.fa.funnel.rest.model.AugmentedDetail;
import com.fa.funnel.rest.model.Sequence;
import static com.fa.funnel.rest.util.Utilities.toISO_UTC;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import static com.fa.funnel.rest.query.SequenceQueryHandler.stripAppIDAndLogtype;
import com.fa.funnel.rest.util.Utilities;

/**
 * Class responsible for generating a response to any POST query made through the application.
 * More specifically, the class returns the following objects depending on the POST query:
 * 
 * <ul>
 * <li> {@link com.fa.funnel.rest.model.QuickStatsResponse} for the 'quick_stats' endpoint</li>
 * <li> {@link com.fa.funnel.rest.model.ExploreResponse} for the 'explore/{mode}' endpoint</li>
 * <li> {@link java.lang.String} for the 'export_completions' endpoint</li>
 * </ul>
 * 
 * @author Andreas Kosmatopoulos, Datalab, A.U.TH.
 */
public class ResponseBuilder
{
    private final Cluster cluster;
    private final Session session;
    private KeyspaceMetadata ks = null;
    private final String cassandra_keyspace_name;

    private static Map<String, List<String>> entityMapping;
    private static List<Step> steps;
    private Date start_date;
    private Date end_date;
    private final long maxDuration;

    private static final String COMMA_DELIMITER = ",";
    private static final String FILE_HEADER = "step"+COMMA_DELIMITER+"completed_at"+COMMA_DELIMITER+"duration"+COMMA_DELIMITER+"app_id"+COMMA_DELIMITER+"device_id"+COMMA_DELIMITER+"user_id";
    
    private static boolean IS_USERS_QUERY;
    
    /**
     * Constructs a ResponseBuilder object. Whenever a new query comes through the application a new ResponseBuilder object is created tailored to the specific query (i.e. specific funnel and start/end dates).
     * The constructor parses the funnel steps and handles the funnel start and end dates.
     * @param cluster The Cassandra cluster connection defined by {@link com.fa.funnel.rest.CassandraConfiguration}
     * @param session The session to the Cassandra cluster
     * @param ks The Cassandra cluster keyspace metadata of the index keyspace
     * @param cassandra_keyspace_name The index keyspace name
     * @param funnel The funnel provided by the application
     * @param from Funnel start date
     * @param till Funnel end date
     */
    public ResponseBuilder(Cluster cluster, Session session, KeyspaceMetadata ks, String cassandra_keyspace_name, Funnel funnel, String from, String till)
    {
        this.cluster = cluster;
        this.session = session;
        this.ks = ks;
        this.cassandra_keyspace_name = cassandra_keyspace_name;

        entityMapping = Collections.synchronizedMap(new HashMap<String, List<String>>());
        
        steps = funnel.getSteps();
        
        IS_USERS_QUERY = isUsersQuery(steps, null);

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        try 
        {
            start_date = formatter.parse(from);
            end_date = formatter.parse(till);
            end_date = new Date(end_date.getTime() + TimeUnit.DAYS.toMillis( 1 ));
        } 
        catch (ParseException ex) 
        {
            Logger.getLogger(ResponseBuilder.class.getName()).log(Level.SEVERE, null, ex);
        }
        maxDuration = funnel.getMaxDuration();
    }
    
    /**
     * Builds a {@link com.fa.funnel.rest.model.QuickStatsResponse} object according to the provided funnel and start/end dates.
     * @return a {@link com.fa.funnel.rest.model.QuickStatsResponse} object corresponding to the JSON information specified for the 'quick_stats' endpoint
     */
    public QuickStatsResponse buildQuickStatsResponse() 
    {
        long tStart = System.currentTimeMillis();
        List<Completion> completions = getCompletions(steps, start_date, end_date, maxDuration);
        long tEnd = System.currentTimeMillis();
        System.out.println("Time Completions (Quick Stats): " + (tEnd - tStart) / 1000.0 + " seconds.");

        QuickStatsResponse result = new QuickStatsResponse();
        result.setCompletions(completions);

        return result;
    }

    /**
     * Builds a {@link com.fa.funnel.rest.model.ExploreResponse} object according to the provided funnel and start/end dates. Furthermore, the parameter {@code strPosition}
     * corresponds to the search location inside the funnel. If it is equal to 0, we are searching for preceding events, if it is less than the funnel's step count we are searching for intermediate 
     * events and if it is equal to the funnel step count we are searching for following events. This evaluation corresponds to the 'accurate' explore method
     * @param strPosition The position in the funnel to which we search for possible continuations. 
     * @param targetAppID If the variable is set, then report results only for the specified application ID
     * @return a {@link com.fa.funnel.rest.model.ExploreResponse} object corresponding to the JSON information specified for the 'explore/accurate' endpoint
     */
    public ExploreResponse buildExploreResponse(String strPosition, String targetAppID)
    {       
        int position;

        if (strPosition.isEmpty())
            position = steps.size();
        else
            position = Integer.parseInt(strPosition);
        
        long tStart = System.currentTimeMillis();
        List<Proposition> followUps = getFollowUps(steps, start_date, end_date, position, targetAppID, maxDuration);
        long tEnd = System.currentTimeMillis();
        System.out.println("Time Propositions (Accurate): " + (tEnd - tStart) / 1000.0 + " seconds.");

        ExploreResponse result = new ExploreResponse();
        result.setPropositions(followUps);

        return result;
    }

    /**
     * Builds a {@link com.fa.funnel.rest.model.ExploreResponse} object according to the provided funnel and start/end dates. Furthermore, the parameter {@code strPosition}
     * corresponds to the search location inside the funnel. If it is equal to 0, we are searching for preceding events, if it is less than the funnel's step count we are searching for intermediate 
     * events and if it is equal to the funnel step count we are searching for following events. This evaluation corresponds to the 'fast' explore method
     * @param strPosition The position in the funnel to which we search for possible continuations. 
     * @param targetAppID If the variable is set, then report results only for the specified application ID
     * @return a {@link com.fa.funnel.rest.model.ExploreResponse} object corresponding to the JSON information specified for the 'explore/fast' endpoint
     */
    public ExploreResponse buildExploreResponseFast(String strPosition, String targetAppID)
    {       
        int position;

        if (strPosition.isEmpty())
            position = steps.size();
        else
            position = Integer.parseInt(strPosition);
        
        long tStart = System.currentTimeMillis();
        int lastCompletions = getCompletionCountOfFullFunnel(steps, start_date, end_date, maxDuration);
        long tEnd = System.currentTimeMillis();
        System.out.println("Time Completion Count Fast: " + (tEnd - tStart) / 1000.0 + " seconds.");
        tStart = System.currentTimeMillis();
        List<Proposition> followUps = getFollowUpsFast(steps, start_date, end_date, position, targetAppID, lastCompletions);
        tEnd = System.currentTimeMillis();
        System.out.println("Time Followups only Fast: " + (tEnd - tStart) / 1000.0 + " seconds.");

        ExploreResponse result = new ExploreResponse();
        result.setPropositions(followUps);

        return result;
    }
    
    /**
     * Builds a {@link com.fa.funnel.rest.model.ExploreResponse} object according to the provided funnel and start/end dates. Furthermore, the parameter {@code strPosition}
     * corresponds to the search location inside the funnel. If it is equal to 0, we are searching for preceding events, if it is less than the funnel's step count we are searching for intermediate 
     * events and if it is equal to the funnel step count we are searching for following events. This evaluation corresponds to the 'hybrid' explore method where the initial results are evaluated using the 'fast' approach
     * and then {@code strTopK} results are evaluated using the 'accurate' approach
     * @param strPosition The position in the funnel to which we search for possible continuations. 
     * @param targetAppID If the variable is set, then report results only for the specified application ID
     * @param strTopK The first 'strTopK' results returned through the 'fast' explore method are then evaluated accurately
     * @return a {@link com.fa.funnel.rest.model.ExploreResponse} object corresponding to the JSON information specified for the 'explore/hybrid' endpoint
     */
    public ExploreResponse buildExploreResponseHybrid(String strPosition, String targetAppID, String strTopK)
    {               
        int position;
        int topK = Integer.parseInt(strTopK);

        if (strPosition.isEmpty())
            position = steps.size();
        else
            position = Integer.parseInt(strPosition);
        
        long tStart = System.currentTimeMillis();
        int lastCompletions = getCompletionCountOfFullFunnel(steps, start_date, end_date, maxDuration);
        List<Proposition> followUps = getFollowUpsHybrid(steps, start_date, end_date, position, targetAppID, maxDuration, lastCompletions, topK);
        long tEnd = System.currentTimeMillis();
        System.out.println("Time Followups Hybrid: " + (tEnd - tStart) / 1000.0 + " seconds.");

        ExploreResponse result = new ExploreResponse();
        result.setPropositions(followUps);

        return result;
    }       
    
    /**
     * Builds a CSV file according to the JSON information specified for the 'export_completions' endpoint
     * @param path the path where the CSV will be saved
     * @return the CSV file name
     */
    public String buildCSVFile(String path)
    {      
        long tStart = System.currentTimeMillis();
        List<DetailedCompletion> completions = getFullRetroactiveCompletions(steps, start_date, end_date, maxDuration);
        long tEnd = System.currentTimeMillis();
        System.out.println("Time Full Retroactive Completions Accurate: " + (tEnd - tStart) / 1000.0 + " seconds.");

        String csvFileName = "csv_" + steps.size() + "-sizedFunnel_" + Utilities.getToday() + ".csv"; //TODO: Should probably include something unique, e.g. time the csv file is going to be built
        try
        {
            PrintWriter pw = new PrintWriter(new File(path + '/' + csvFileName));
            pw.println(FILE_HEADER);
            for (DetailedCompletion dc : completions)
            {
                StringBuilder sb = new StringBuilder();
                sb.append(dc.getStep()).append(COMMA_DELIMITER).append(toISO_UTC(dc.getCompleted_at())).append(COMMA_DELIMITER).append(dc.getDuration()).append(COMMA_DELIMITER)
                        .append(dc.getApp_id()).append(COMMA_DELIMITER).append(dc.getDevice_id()).append(COMMA_DELIMITER).append(dc.getUser_id());
                pw.println(sb.toString());
            }
            pw.close();
        }
        catch (FileNotFoundException ex)
        {
            Logger.getLogger(ResponseBuilder.class.getName()).log(Level.SEVERE, null, ex);
        }

        return csvFileName;
    }

    private List<Step> copyOriginalStepList(List<Step> steps) 
    {
        List<Step> thecopy = new ArrayList<Step>();
        
        for (Step step : steps)
        {
            Step newstep = new Step();
            
            List<Name> newnames = new ArrayList<Name>();
            
            List<Name> names = step.getMatchName();
                        
            for (Name name : names)
                newnames.add(name);
            
            newstep.setMatchName(newnames);
            newstep.setMatchDetails(step.getMatchDetails());
            
            thecopy.add(newstep);
        }
        
        return thecopy;
        
    }

    private Map<Sequence, Map<Integer, List<AugmentedDetail>>> generateAllSubqueries(List<List<Step>> listOfSteps) 
    {
        Map<Sequence, Map<Integer, List<AugmentedDetail>>> allQueries = new HashMap<Sequence, Map<Integer, List<AugmentedDetail>>>();

        for (List<Step> list : listOfSteps)
        {
            List<Sequence> allQueriesForList = new ArrayList<Sequence>(); // This holds all the possible subqueries
            List<Map<Integer, List<AugmentedDetail>>> allDetailsForList = new ArrayList<Map<Integer, List<AugmentedDetail>>>(); // This holds all the details for all sub-queries
            
            for (int i = 0; i < list.size(); i++)
            {
                Sequence subquery;
                Map<Integer, List<AugmentedDetail>> subdetails;
                if (i == 0)
                {
                    subquery = new Sequence();
                    subdetails = new HashMap<Integer, List<AugmentedDetail>>();
                }
                else
                {
                    subquery = new Sequence(allQueriesForList.get(i - 1));
                    subdetails = allDetailsForList.get(i - 1).entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> new ArrayList<AugmentedDetail>(e.getValue())));
                }

                Step step = list.get(i);

                String eventName = step.getMatchName().get(0).getApplicationID() + "_" + step.getMatchName().get(0).getLogType() + "_" + step.getMatchName().get(0).getLogName();

                List<AugmentedDetail> details = transformToAugmentedDetails(eventName, step.getMatchDetails());

                subquery.appendToSequence(new Event(eventName, new TreeSet<AugmentedDetail>(details)));

                if (details.size() > 0)
                    subdetails.put(i, details);
                allQueriesForList.add(subquery);
                allDetailsForList.add(subdetails);
                allQueries.put(subquery, subdetails);
            }
        }
        
        return allQueries;
    }
    
    private int getCompletionCountOfFullFunnel(List<Step> steps, Date start_date, Date end_date, long maxDuration) 
    {
        List<List<Step>> listOfSteps = simplifySequences(steps);
        
        int totalCount = 0;
        
        for (List<Step> stepsList : listOfSteps)
        {
            SequenceQueryExplorer sqex = new SequenceQueryExplorer(cluster, session, ks, cassandra_keyspace_name);

            Sequence query = new Sequence();
            Map<Integer, List<AugmentedDetail>> queryDetails = new HashMap<Integer, List<AugmentedDetail>>();

            for (int i = 0; i < stepsList.size(); i++)
            {
                Step step = stepsList.get(i);

                String eventName = step.getMatchName().get(0).getApplicationID() + "_" + step.getMatchName().get(0).getLogType() + "_" + step.getMatchName().get(0).getLogName();

                List<AugmentedDetail> details = transformToAugmentedDetails(eventName, step.getMatchDetails());

                query.appendToSequence(new Event(eventName, new TreeSet<AugmentedDetail>(details)));

                if (details.size() > 0)
                    queryDetails.put(i, details);
            }

            if (query.getSize() == 1 && queryDetails.isEmpty())
                return Integer.MAX_VALUE;

            List<QueryPair> qps = query.getQueryTuples();
            
            int partialCount = Integer.MAX_VALUE;
            
            for (QueryPair qp : qps)
            {
                int count = sqex.getCountForQueryPair(start_date, end_date, qp, IS_USERS_QUERY);
                if (count < partialCount)
                    partialCount = count;
            }
            
            for (Integer step : queryDetails.keySet())
            {
                List<AugmentedDetail> stepDetails = queryDetails.get(step);
                
                for (AugmentedDetail det : stepDetails)
                {
                    int count = sqex.getCountForEventKeyValueTriplet(start_date, end_date, det.evName, det.key, det.value, IS_USERS_QUERY);
                    if (count < partialCount)
                        partialCount = count;                    
                }
            }
            
            totalCount = totalCount + partialCount;
        }

        return totalCount;
    }

    private List<Completion> getCompletions(List<Step> steps, Date start_date, Date end_date, long maxDuration)
    {
        List<List<Step>> listOfSteps = simplifySequences(steps);
        
        Map<Sequence, Map<Integer, List<AugmentedDetail>>> allQueries = generateAllSubqueries(listOfSteps);

        Map<Integer, Map<String, Long>> allTruePositives = new TreeMap<Integer, Map<String, Long>>();
        
        for (Map.Entry<Sequence, Map<Integer, List<AugmentedDetail>>> entry : allQueries.entrySet())
        {
            SequenceQueryEvaluator sqev = new SequenceQueryEvaluator(cluster, session, ks, cassandra_keyspace_name);

            Sequence query = entry.getKey();
            Map<Integer, List<AugmentedDetail>> queryDetails = entry.getValue();
            
            if (query.getSize() == 1)
                continue;

            Set<String> candidates = sqev.evaluateQuery(start_date, end_date, query, queryDetails, IS_USERS_QUERY);
            Map<String, Long> truePositives = sqev.findTruePositives(query, candidates, maxDuration);

            int step = query.getSize()-1;
            if (!allTruePositives.containsKey(step))
                allTruePositives.put(step, new HashMap<String, Long>());
            Long oldMaxDate = allTruePositives.get(step).get("maxDate");
            allTruePositives.get(step).putAll(truePositives);
            if (oldMaxDate != null && oldMaxDate > allTruePositives.get(step).get("maxDate"))
                allTruePositives.get(step).put("maxDate", oldMaxDate);
        }
        
        List<Completion> completions = new ArrayList<Completion>();

        for (Map.Entry<Integer, Map<String,Long>> entry : allTruePositives.entrySet())
        {
            int step = entry.getKey();
            Map<String, Long> truePositives = entry.getValue();
            
            int count = truePositives.keySet().size() - 1;
            long totalDuration = 0;
            for (String sessionID : truePositives.keySet()) 
            {
                if (!sessionID.equals("maxDate"))
                    totalDuration = totalDuration + truePositives.get(sessionID);
            }

            Double avgDuration;
            String lastCompletedAt;

            if (count != 0)
            {
                avgDuration = (totalDuration / (truePositives.keySet().size() - 1)) / 1000.0;
                lastCompletedAt = toISO_UTC(new Date(truePositives.get("maxDate")));
            }
            else
            {
                avgDuration = null;
                lastCompletedAt = null;
            }

            Completion completion = new Completion(step, count, avgDuration, lastCompletedAt);

            completions.add(completion);
        }

        return completions;
    }
    
    private List<Proposition> getFollowUps(List<Step> steps, Date start_date, Date end_date, int position, String targetAppID, long maxDuration)
    {
        List<List<Step>> listOfSteps = simplifySequences(steps);
        
        Map<String, Map<String, Long>> allTruePositives = new TreeMap<String, Map<String, Long>>();
        
        for (List<Step> simpSteps : listOfSteps)
        {
            Sequence query = new Sequence();
            Map<Integer, List<AugmentedDetail>> details = new HashMap<Integer, List<AugmentedDetail>>();

            for (int i = 0; i < simpSteps.size(); i++)
            {
                Step step = simpSteps.get(i);

                String eventName = step.getMatchName().get(0).getApplicationID() + "_" + step.getMatchName().get(0).getLogType() + "_" + step.getMatchName().get(0).getLogName();

                List<AugmentedDetail> stepdetails = transformToAugmentedDetails(eventName, step.getMatchDetails());

                query.appendToSequence(new Event(eventName, new TreeSet<AugmentedDetail>(stepdetails)));
                if (stepdetails.size() > 0)
                    details.put(i, stepdetails);
            }

            SequenceQueryExplorer sqex = new SequenceQueryExplorer(cluster, session, ks, cassandra_keyspace_name);
            Map<String, Map<String, Long>> followUps = sqex.exploreQueryAccurateStep(start_date, end_date, query, details, position, targetAppID, maxDuration, IS_USERS_QUERY);
            
            for (String event : followUps.keySet())
            {
                if (!allTruePositives.containsKey(event))
                    allTruePositives.put(event, new HashMap<String, Long>());
                Long oldMaxDate = allTruePositives.get(event).get("maxDate");
                allTruePositives.get(event).putAll(followUps.get(event));
                if (oldMaxDate != null && oldMaxDate > allTruePositives.get(event).get("maxDate"))
                    allTruePositives.get(event).put("maxDate", oldMaxDate);
            }
        }
        
        List<Proposition> followUps = new ArrayList<Proposition>();
        
        for (Map.Entry<String, Map<String,Long>> entry : allTruePositives.entrySet())
        {
            String event = entry.getKey();
            Map<String, Long> truePositives = entry.getValue();        
        
            int count = truePositives.keySet().size() - 1;
            long totalDuration = 0;
            for (String sessionID : truePositives.keySet())
                if (!sessionID.equals("maxDate"))
                    totalDuration = totalDuration + truePositives.get(sessionID);

            Double avgDuration;
            String lastCompletedAt;
            if (count != 0)
            {
                avgDuration = (totalDuration / (truePositives.keySet().size() - 1)) / 1000.0;
                lastCompletedAt = toISO_UTC(new Date(truePositives.get("maxDate")));
            }
            else
            {
                avgDuration = null;
                lastCompletedAt = null;
            }

            Proposition prop = new Proposition(Integer.parseInt(event.split("_")[0]), Integer.parseInt(event.split("_")[1]), stripAppIDAndLogtype(event), count, avgDuration, lastCompletedAt);

            if (count != 0) // Only show interesting results
                followUps.add(prop);
        }
        Collections.sort(followUps);
        return followUps;
    }

    private List<Proposition> getFollowUpsFast(List<Step> steps, Date start_date, Date end_date, int position, String targetAppID, int lastCompletions)
    {
        List<List<Step>> listOfSteps = simplifySequences(steps);
        
        Map<String, Proposition> allPropositions = new HashMap<String, Proposition>();
        List<Proposition> results = Collections.emptyList();
        
        for (List<Step> simpList : listOfSteps)
        {
            Sequence query = new Sequence();
            Map<Integer, List<AugmentedDetail>> details = new HashMap<Integer, List<AugmentedDetail>>();

            for (int i = 0; i < simpList.size(); i++)
            {
                Step step = simpList.get(i);

                String eventName = step.getMatchName().get(0).getApplicationID() + "_" + step.getMatchName().get(0).getLogType() + "_" + step.getMatchName().get(0).getLogName();

                List<AugmentedDetail> stepdetails = transformToAugmentedDetails(eventName, step.getMatchDetails());

                query.appendToSequence(new Event(eventName, new TreeSet<AugmentedDetail>(stepdetails)));
                if (stepdetails.size() > 0)
                    details.put(i, stepdetails);
            }

            SequenceQueryExplorer sqex = new SequenceQueryExplorer(cluster, session, ks, cassandra_keyspace_name);
            List<Proposition> followUps = sqex.exploreQueryFastStep(start_date, end_date, query, details, position, targetAppID, lastCompletions, IS_USERS_QUERY);
            
            for (Proposition prop : followUps)
            {
                String event = prop.getApplicationID() + "_" + prop.getLogName();
                if (!allPropositions.containsKey(event))
                    allPropositions.put(event, new Proposition(prop.getApplicationID(), prop.getLogType(), stripAppIDAndLogtype(event), 0));
                
                Proposition existingProp = allPropositions.get(event);
                existingProp.setCompletions(existingProp.getCompletions() + prop.getCompletions());
                if (existingProp.getCompletions() > lastCompletions)
                    existingProp.setCompletions(lastCompletions);
                if (existingProp.getAverageDuration() == null || prop.getAverageDuration() > existingProp.getAverageDuration())
                    existingProp.setAverageDuration(prop.getAverageDuration());
            }
            
            results = new ArrayList<Proposition>(allPropositions.values());
            
            Collections.sort(results);
        }

        return results;
    }
    
    private List<Proposition> getFollowUpsHybrid(List<Step> steps, Date start_date, Date end_date, int position, String targetAppID, long maxDuration, int lastCompletions, int topK)
    {
        List<List<Step>> listOfSteps = simplifySequences(steps);
        
        List<Sequence> allQueries = new ArrayList<Sequence>();
        Map<Integer, List<AugmentedDetail>> details = Collections.emptyMap();
        
        for (List<Step> simpSteps : listOfSteps)
        {
            Sequence query = new Sequence();
            details = new HashMap<Integer, List<AugmentedDetail>>(); // WARNING: This actually gets rewritten in every loop but since all details are the same for each "simpSteps" list it doesn't matter

            for (int i = 0; i < simpSteps.size(); i++)
            {
                Step step = simpSteps.get(i);

                String eventName = step.getMatchName().get(0).getApplicationID() + "_" + step.getMatchName().get(0).getLogType() + "_" + step.getMatchName().get(0).getLogName();

                List<AugmentedDetail> stepdetails = transformToAugmentedDetails(eventName, step.getMatchDetails());

                query.appendToSequence(new Event(eventName, new TreeSet<AugmentedDetail>(stepdetails)));
                if (stepdetails.size() > 0)
                    details.put(i, stepdetails);
            }
            
            allQueries.add(query);
        }
        
        List<Proposition> allFastFollowups = getFollowUpsFast(steps, start_date, end_date, position, targetAppID, lastCompletions);
        
        List<Proposition> results = new ArrayList<Proposition>();
        
        int propCount = 0;
        
        Iterator<Proposition> it = allFastFollowups.iterator();
        while (it.hasNext())
        {
            Proposition prop = it.next();
            
            if (propCount < topK) // Accurate evaluation
            {
                String event = prop.getApplicationID() + "_" + prop.getLogType() + "_" + prop.getLogName();
                Map<String, Long> allTruePositives = new TreeMap<String, Long>();

                for (Sequence query : allQueries)
                {
                    SequenceQueryEvaluator sqev = new SequenceQueryEvaluator(cluster, session, ks, cassandra_keyspace_name);
                    
                    query.appendToSequence(new Event(event));                    

                    Set<String> candidates = sqev.evaluateQuery(start_date, end_date, query, details, IS_USERS_QUERY);
                    Map<String, Long> truePositives = sqev.findTruePositives(query, candidates, maxDuration);

                    Long oldMaxDate = allTruePositives.get("maxDate");
                    allTruePositives.putAll(truePositives);
                    if (oldMaxDate != null && oldMaxDate > allTruePositives.get("maxDate"))
                        allTruePositives.put("maxDate", oldMaxDate);
                    
                    query.removeLastEvent();
                }
                
                int count = allTruePositives.keySet().size() - 1;
                long totalDuration = 0;
                for (String sessionID : allTruePositives.keySet())
                    if (!sessionID.equals("maxDate"))
                        totalDuration = totalDuration + allTruePositives.get(sessionID);

                Double avgDuration;
                String lastCompletedAt;
                if (count != 0)
                {
                    avgDuration = (totalDuration / (allTruePositives.keySet().size() - 1)) / 1000.0;
                    lastCompletedAt = toISO_UTC(new Date(allTruePositives.get("maxDate")));
                }
                else
                {
                    avgDuration = null;
                    lastCompletedAt = null;
                }

                Proposition newprop = new Proposition(Integer.parseInt(event.split("_")[0]), Integer.parseInt(event.split("_")[1]), stripAppIDAndLogtype(event), count, avgDuration, lastCompletedAt);

                if (count != 0) // Only show interesting results
                {
                    results.add(newprop);
                    propCount++;
                }
            }
            else
                results.add(prop);
        }
        
        return results;
    }

    private List<DetailedCompletion> getFullRetroactiveCompletions(List<Step> steps, Date start_date, Date end_date, long maxDuration)
    {
        List<List<Step>> listOfSteps = simplifySequences(steps);
        Map<Sequence, Map<Integer, List<AugmentedDetail>>> allQueries = generateAllSubqueries(listOfSteps);
        
        Map<Integer, Map<String, Lifetime>> allTruePositives = new TreeMap<Integer, Map<String, Lifetime>>();

        List<DetailedCompletion> completions = new ArrayList<DetailedCompletion>();

        for (Map.Entry<Sequence, Map<Integer, List<AugmentedDetail>>> entry : allQueries.entrySet())
        {
            SequenceQueryEvaluator sqev = new SequenceQueryEvaluator(cluster, session, ks, cassandra_keyspace_name);

            Sequence query = entry.getKey();
            Map<Integer, List<AugmentedDetail>> queryDetails = entry.getValue();

            Set<String> candidates = sqev.evaluateQuery(start_date, end_date, query, queryDetails, IS_USERS_QUERY);
            Map<String, Lifetime> truePositives = sqev.findTruePositivesAndLifetime(query, candidates, maxDuration);
            
            int step = query.getSize()-1;
            if (!allTruePositives.containsKey(step))
                allTruePositives.put(step, new HashMap<String, Lifetime>());
            allTruePositives.get(step).putAll(truePositives);
            
            if (IS_USERS_QUERY)
                entityMapping.putAll(sqev.getDeviceIDs(start_date, end_date, truePositives.keySet()));
            else
                entityMapping.putAll(sqev.getUserIDs(start_date, end_date, truePositives.keySet()));
        }
        
        for (Map.Entry<Integer, Map<String,Lifetime>> entry : allTruePositives.entrySet())
        {
            int i = entry.getKey();
            Map<String, Lifetime> truePositives = entry.getValue();
            
            for (String entity_id : truePositives.keySet())
            {
                Lifetime lt = truePositives.get(entity_id);
                String device_id, user_id;
                if (IS_USERS_QUERY)
                {
                    device_id = (entityMapping.containsKey(entity_id) ? getRandomEntity(entityMapping.get(entity_id), lt.appID) : null);
                    user_id = entity_id;
                }
                else
                {
                    device_id = entity_id;
                    user_id = (entityMapping.containsKey(entity_id) ? getRandomEntity(entityMapping.get(entity_id), null) : null);
                }

                DetailedCompletion dc = new DetailedCompletion(i, lt.end_date, (lt.duration / 1000), null, lt.appID, device_id.replaceFirst("(\\d)+_", ""), user_id);

                completions.add(dc);
            }
        }

        return completions;
    }

    private boolean isUsersQuery(List<Step> steps, String appID) 
    {
        List<List<Step>> allLists = simplifySequences(steps);
        
        for (List<Step> stepList : allLists)
        {
            HashSet<String> allAppIDs = new HashSet<String>();
            if (appID != null && !appID.isEmpty())
                allAppIDs.add(appID);
            
            for (Step step : stepList)
            {
                allAppIDs.add(step.getMatchName().get(0).getApplicationID());
                if (allAppIDs.size() > 1)
                    return true;
            }
        }
        
        return false;
    }

    /**
     * Simplify a list of steps potentially containing OR disjunctions between steps to a list of list of steps
     * that avoid OR disjunctions between their steps
     * @param steps A list of the steps contained in the original funnel
     * @return A list of list of steps, each containing simple steps without logical operators between them
     */
    public List<List<Step>> simplifySequences(List<Step> steps) 
    {
        List<List<Step>> allLists = new ArrayList<List<Step>>();
        
        Step firstStep = steps.get(0);
        
        // Start a new list for each Name in the first step of the funnel
        for (Name name : firstStep.getMatchName())
        {
            List<Name> singleNameList = new ArrayList<Name>();
            singleNameList.add(name);
            
            Step singleNameStep = new Step();
            singleNameStep.setMatchName(singleNameList);
            singleNameStep.setMatchDetails(firstStep.getMatchDetails());
            
            List<Step> newList = new ArrayList<Step>();
            newList.add(singleNameStep);
            
            allLists.add(newList);
        }
        
        // Add each name in each subsequent step in the currently partially completed lists
        for (int i=1; i<steps.size(); i++)
        {
            List<List<Step>> tempAllLists = new ArrayList<List<Step>>();
            Step curStep = steps.get(i);
            for (Name name : curStep.getMatchName())
            {
                for (List<Step> list : allLists)
                {
                    List<Step> copied = copyOriginalStepList(list);
                    List<Name> singleNameList = new ArrayList<Name>();
                    singleNameList.add(name);

                    Step singleNameStep = new Step();
                    singleNameStep.setMatchName(singleNameList);
                    singleNameStep.setMatchDetails(curStep.getMatchDetails());

                    copied.add(singleNameStep);

                    tempAllLists.add(copied);
                }
            }
            allLists = new ArrayList<List<Step>>(tempAllLists);
        }
        return allLists;
    }
    
    /**
     * Transform each Detail to AugmentedDetail by incorporating the corresponding event name
     * @param eventName The event name of the AugmentedDetail
     * @param details A list of details for this step
     * @return The corresponding list of AugmentedDetail
     */
    protected List<AugmentedDetail> transformToAugmentedDetails(String eventName, List<Detail> details)
    {
        List<AugmentedDetail> stepDetails = new ArrayList<AugmentedDetail>();

        if (details == null)
            return Collections.emptyList();

        for (int i = 0; i < details.size(); i++)
        {
            Detail detail = details.get(i);

            stepDetails.add(new AugmentedDetail(eventName, detail.getKey(), detail.getValue()));
        }
        return stepDetails;
    }

    private String getRandomEntity(List<String> entities, String appID)
    {
        Random rnd = new Random();
        
        if (!IS_USERS_QUERY || appID == null) // Device query. Both should be true
        {
            int index = rnd.nextInt(entities.size());
            return entities.get(index);
        }
        else // User query.
        {
            List<String> relevantEntitites = new ArrayList<String>(entities);
            Iterator<String> it = relevantEntitites.iterator();
            while (it.hasNext()) // Filter out any devices not prefixed with the particular app ID
            {
                String entity = it.next();
                if (!entity.startsWith(appID))
                    it.remove();
            }
            if (relevantEntitites.isEmpty()) // This means that the user has a number of devices but none of them belong to this particular app
                return "null";
            
            int index = rnd.nextInt(relevantEntitites.size());
            return relevantEntitites.get(index);
        }
    }
}
