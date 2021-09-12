package com.sequence.detection.rest.query;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.sequence.detection.rest.model.*;
import com.sequence.detection.rest.model.AugmentedDetail;
import com.sequence.detection.rest.model.Sequence;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.sequence.detection.rest.util.Utilities;
import com.sequence.detection.rest.CassandraConfiguration;

/**
 * Class responsible for generating a response to any POST query made through the application.
 * More specifically, the class returns the following objects depending on the POST query:
 *
 * <ul>
 * <li> {@link QuickStatsResponse} for the 'quick_stats' endpoint</li>
 * <li> {@link ExploreResponse} for the 'explore/{mode}' endpoint</li>
 * <li> {@link java.lang.String} for the 'export_completions' endpoint</li>
 * </ul>
 *
 * @author Andreas Kosmatopoulos, Datalab, A.U.TH.
 */
public class ResponseBuilder {
    protected final Cluster cluster;
    protected final Session session;
    protected KeyspaceMetadata ks;
    protected final String cassandra_keyspace_name;

    protected static List<Step> steps;
    protected static String tableLogName;
    protected static String table_name;
    protected Date start_date;
    protected Date end_date;
    protected final long maxDuration;
    /**
     * Constructs a ResponseBuilder object. Whenever a new query comes through the application a new ResponseBuilder object is created tailored to the specific query (i.e. specific funnel and start/end dates).
     * The constructor parses the funnel steps and handles the funnel start and end dates.
     *
     * @param cluster                 The Cassandra cluster connection defined by {@link CassandraConfiguration}
     * @param session                 The session to the Cassandra cluster
     * @param ks                      The Cassandra cluster keyspace metadata of the index keyspace
     * @param cassandra_keyspace_name The index keyspace name
     * @param funnel                  The funnel provided by the application
     * @param from                    Funnel start date
     * @param till                    Funnel end date
     */
    public ResponseBuilder(Cluster cluster, Session session, KeyspaceMetadata ks, String cassandra_keyspace_name, Funnel funnel, String from, String till) {
        this.cluster = cluster;
        this.session = session;
        this.ks = ks;
        this.cassandra_keyspace_name = cassandra_keyspace_name;

        Map<String, List<String>> entityMapping = Collections.synchronizedMap(new HashMap<>());

        steps = funnel.getSteps();
        tableLogName = funnel.getLogName().toLowerCase().replace(".", ",").split(",")[0].replace(" ", "_") + "_idx";
        table_name = funnel.getLogName().toLowerCase().replace(".", ",").split(",")[0].replace(" ", "_");

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        try {
            start_date = formatter.parse(from);
            end_date = formatter.parse(till);
            end_date = new Date(end_date.getTime() + TimeUnit.DAYS.toMillis(1));
        } catch (ParseException ex) {
            Logger.getLogger(ResponseBuilder.class.getName()).log(Level.SEVERE, null, ex);
        }
        maxDuration = funnel.getMaxDuration();
    }

    public DetectionResponse buildDetectionResponse() {
        long tStart = System.currentTimeMillis();
        List<DetectedSequence> ids = getDetections(steps, start_date, end_date, maxDuration, tableLogName);
        long tEnd = System.currentTimeMillis();
        System.out.println("Time Completions (Detection): " + (tEnd - tStart) / 1000.0 + " seconds.");

        DetectionResponse result = new DetectionResponse();
        result.setIds(ids);

        return result;
    }

    /**
     * Builds a {@link QuickStatsResponse} object according to the provided funnel and start/end dates.
     *
     * @return a {@link QuickStatsResponse} object corresponding to the JSON information specified for the 'quick_stats' endpoint
     */
    public QuickStatsResponse buildQuickStatsResponse() {
        long tStart = System.currentTimeMillis();
        List<Completion> completions = getCompletions(steps, start_date, end_date, maxDuration, tableLogName);
        long tEnd = System.currentTimeMillis();
        System.out.println("Time Completions (Quick Stats): " + (tEnd - tStart) / 1000.0 + " seconds.");

        QuickStatsResponse result = new QuickStatsResponse();
        result.setCompletions(completions);

        return result;
    }

    /**
     * Builds a {@link ExploreResponse} object according to the provided funnel and start/end dates. Furthermore, the parameter {@code strPosition}
     * corresponds to the search location inside the funnel. If it is equal to 0, we are searching for preceding events, if it is less than the funnel's step count we are searching for intermediate
     * events and if it is equal to the funnel step count we are searching for following events. This evaluation corresponds to the 'accurate' explore method
     *
     * @param strPosition The position in the funnel to which we search for possible continuations.
     * @return a {@link ExploreResponse} object corresponding to the JSON information specified for the 'explore/accurate' endpoint
     */
    public ExploreResponse buildExploreResponse(String strPosition) {
        int position;

        if (strPosition.isEmpty())
            position = steps.size();
        else
            position = Integer.parseInt(strPosition);

        long tStart = System.currentTimeMillis();
        List<Proposition> followingEvents = getExploreEvents(steps, start_date, end_date, table_name, position, maxDuration);
        long tEnd = System.currentTimeMillis();
        System.out.println("Time Propositions (Accurate): " + (tEnd - tStart) / 1000.0 + " seconds.");

        ExploreResponse result = new ExploreResponse();
        result.setPropositions(followingEvents);

        return result;
    }

    /**
     * Builds a {@link ExploreResponse} object according to the provided funnel and start/end dates. Furthermore, the parameter {@code strPosition}
     * corresponds to the search location inside the funnel. If it is equal to 0, we are searching for preceding events, if it is less than the funnel's step count we are searching for intermediate
     * events and if it is equal to the funnel step count we are searching for following events. This evaluation corresponds to the 'fast' explore method
     *
     * @param strPosition The position in the funnel to which we search for possible continuations.
     * @return a {@link ExploreResponse} object corresponding to the JSON information specified for the 'explore/fast' endpoint
     */
    public ExploreResponse buildExploreResponseFast(String strPosition) {
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
        List<Proposition> propositions = getExploreFast(steps, start_date, end_date, position, lastCompletions);
        tEnd = System.currentTimeMillis();
        System.out.println("Time explore fast: " + (tEnd - tStart) / 1000.0 + " seconds.");

        ExploreResponse result = new ExploreResponse();
        result.setPropositions(propositions);

        return result;
    }

    /**
     * Builds a {@link ExploreResponse} object according to the provided funnel and start/end dates. Furthermore, the parameter {@code strPosition}
     * corresponds to the search location inside the funnel. If it is equal to 0, we are searching for preceding events, if it is less than the funnel's step count we are searching for intermediate
     * events and if it is equal to the funnel step count we are searching for following events. This evaluation corresponds to the 'hybrid' explore method where the initial results are evaluated using the 'fast' approach
     * and then {@code strTopK} results are evaluated using the 'accurate' approach
     *
     * @param strPosition The position in the funnel to which we search for possible continuations.
     * @param strTopK     The first 'strTopK' results returned through the 'fast' explore method are then evaluated accurately
     * @return a {@link ExploreResponse} object corresponding to the JSON information specified for the 'explore/hybrid' endpoint
     */
    public ExploreResponse buildExploreResponseHybrid(String strPosition, String strTopK) {
        int position;
        int topK = Integer.parseInt(strTopK);

        if (strPosition.isEmpty())
            position = steps.size();
        else
            position = Integer.parseInt(strPosition);

        long tStart = System.currentTimeMillis();
        int lastCompletions = getCompletionCountOfFullFunnel(steps, start_date, end_date, maxDuration);
        List<Proposition> propositions = getExploreHybrid(steps, start_date, end_date,tableLogName,position, maxDuration, lastCompletions, topK);
        long tEnd = System.currentTimeMillis();
        System.out.println("Time explore Hybrid: " + (tEnd - tStart) / 1000.0 + " seconds.");

        ExploreResponse result = new ExploreResponse();
        result.setPropositions(propositions);

        return result;
    }


    private List<Step> copyOriginalStepList(List<Step> steps) {
        List<Step> thecopy = new ArrayList<>();

        for (Step step : steps) {
            Step newstep = new Step();

            List<Name> newnames = new ArrayList<>();

            List<Name> names = step.getMatchName();

            newnames.addAll(names);

            newstep.setMatchName(newnames);
            newstep.setMatchDetails(step.getMatchDetails());

            thecopy.add(newstep);
        }

        return thecopy;

    }

    protected Map<Sequence, Map<Integer, List<AugmentedDetail>>> generateAllSubqueriesWithoutAppName(List<List<Step>> listOfSteps) {
        Map<Sequence, Map<Integer, List<AugmentedDetail>>> allQueries = new HashMap<>();

        for (List<Step> list : listOfSteps) {
            List<Sequence> allQueriesForList = new ArrayList<Sequence>(); // This holds all the possible subqueries
            List<Map<Integer, List<AugmentedDetail>>> allDetailsForList = new ArrayList<Map<Integer, List<AugmentedDetail>>>(); // This holds all the details for all sub-queries

            for (int i = 0; i < list.size(); i++) {
                Sequence subquery;
                Map<Integer, List<AugmentedDetail>> subdetails;
                if (i == 0) {
                    subquery = new Sequence();
                    subdetails = new HashMap<>();
                } else {
                    subquery = new Sequence(allQueriesForList.get(i - 1));
                    subdetails = allDetailsForList.get(i - 1).entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> new ArrayList<AugmentedDetail>(e.getValue())));
                }

                Step step = list.get(i);

                String eventName = step.getMatchName().get(0).getActivityName();

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

    private int getCompletionCountOfFullFunnel(List<Step> steps, Date start_date, Date end_date, long maxDuration) {
        List<List<Step>> listOfSteps = simplifySequences(steps);

        int totalCount = 0;

        for (List<Step> stepsList : listOfSteps) {
            SequenceExplorer sqex = new SequenceExplorer(cluster, session, ks, cassandra_keyspace_name);

            Sequence query = new Sequence();
            Map<Integer, List<AugmentedDetail>> queryDetails = new HashMap<Integer, List<AugmentedDetail>>();

            for (int i = 0; i < stepsList.size(); i++) {
                Step step = stepsList.get(i);

                String eventName = step.getMatchName().get(0).getActivityName();

                List<AugmentedDetail> details = transformToAugmentedDetails(eventName, step.getMatchDetails());

                query.appendToSequence(new Event(eventName, new TreeSet<AugmentedDetail>(details)));

                if (details.size() > 0)
                    queryDetails.put(i, details);
            }

            if (query.getSize() == 1 && queryDetails.isEmpty())
                return Integer.MAX_VALUE;

            List<QueryPair> qps = query.getQueryTuples();

            int partialCount = Integer.MAX_VALUE;

            for (QueryPair qp : qps) {
                int count = sqex.getCountForQueryPair(start_date, end_date, qp, table_name);
                if (count < partialCount)
                    partialCount = count;
            }

            totalCount = totalCount + partialCount;
        }

        return totalCount;
    }

    private List<DetectedSequence> getDetections(List<Step> steps, Date start_date, Date end_date, long maxDuration, String tableName) {
        List<List<Step>> listOfSteps = simplifySequences(steps);
        Map<Sequence, Map<Integer, List<AugmentedDetail>>> allQueries = generateAllSubqueriesWithoutAppName(listOfSteps);
        Map<Integer, Map<String, Lifetime>> allTruePositives = new TreeMap<Integer, Map<String, Lifetime>>();
        List<DetectedSequence> detectedSequences = new ArrayList<>();

        for (Map.Entry<Sequence, Map<Integer, List<AugmentedDetail>>> entry : allQueries.entrySet()) {
            SequenceQueryEvaluator sqev = new SequenceQueryEvaluator(cluster, session, ks, cassandra_keyspace_name);

            Sequence query = entry.getKey();
            if(query.getList().size()!=steps.size()){ //just return the whole query, without all the subqueries
                continue;
            }

            Map<Integer, List<AugmentedDetail>> queryDetails = entry.getValue();

            if (query.getSize() == 1)
                continue;
            Set<String> candidates = sqev.evaluateQueryLogFile(start_date, end_date, query, queryDetails, tableName);
            Map<String, Lifetime> truePositives = sqev.findTruePositivesAndLifetime(query, candidates, maxDuration);
            detectedSequences.add(new DetectedSequence(truePositives, query.toString()));
            int step = query.getSize() - 1;
            if (!allTruePositives.containsKey(step))
                allTruePositives.put(step, new HashMap<String, Lifetime>());
            allTruePositives.get(step).putAll(truePositives);

        }

        return detectedSequences;
    }


    private List<Completion> getCompletions(List<Step> steps, Date start_date, Date end_date, long maxDuration, String tableName) {
        List<List<Step>> listOfSteps = simplifySequences(steps);

        Map<Sequence, Map<Integer, List<AugmentedDetail>>> allQueries = generateAllSubqueriesWithoutAppName(listOfSteps);

        Map<Integer, Map<String, Long>> allTruePositives = new TreeMap<Integer, Map<String, Long>>();

        for (Map.Entry<Sequence, Map<Integer, List<AugmentedDetail>>> entry : allQueries.entrySet()) {
            SequenceQueryEvaluator sqev = new SequenceQueryEvaluator(cluster, session, ks, cassandra_keyspace_name);

            Sequence query = entry.getKey();
            Map<Integer, List<AugmentedDetail>> queryDetails = entry.getValue();

            if (query.getSize() == 1)
                continue;

            Set<String> candidates = sqev.evaluateQueryLogFile(start_date, end_date, query, queryDetails, tableName);

            Map<String, Long> truePositives = sqev.findTruePositives(query, candidates, maxDuration);

            int step = query.getSize() - 1;
            if (!allTruePositives.containsKey(step))
                allTruePositives.put(step, new HashMap<String, Long>());
            Long oldMaxDate = allTruePositives.get(step).get("maxDate");
            allTruePositives.get(step).putAll(truePositives);
            if (oldMaxDate != null && oldMaxDate > allTruePositives.get(step).get("maxDate"))
                allTruePositives.get(step).put("maxDate", oldMaxDate);
        }

        List<Completion> completions = new ArrayList<Completion>();

        for (Map.Entry<Integer, Map<String, Long>> entry : allTruePositives.entrySet()) {
            int step = entry.getKey();
            Map<String, Long> truePositives = entry.getValue();

            int count = truePositives.keySet().size() - 1;
            long totalDuration = 0;
            for (String sessionID : truePositives.keySet()) {
                if (!sessionID.equals("maxDate"))
                    totalDuration = totalDuration + truePositives.get(sessionID);
            }

            Double avgDuration;
            String lastCompletedAt;

            if (count != 0) {
                avgDuration = (totalDuration / (truePositives.keySet().size() - 1)) / 1000.0;
                lastCompletedAt = Utilities.toISO_UTC(new Date(truePositives.get("maxDate")));
            } else {
                avgDuration = null;
                lastCompletedAt = null;
            }

            Completion completion = new Completion(step, count, avgDuration, lastCompletedAt);

            completions.add(completion);
        }

        return completions;
    }

    private List<Proposition> getExploreEvents(List<Step> steps, Date start_date, Date end_date, String table_name, int position, long maxDuration) {
        List<List<Step>> listOfSteps = simplifySequences(steps);

        Map<String, Map<String, Long>> allTruePositives = new TreeMap<String, Map<String, Long>>();

        for (List<Step> simpSteps : listOfSteps) {
            Sequence query = new Sequence();
            Map<Integer, List<AugmentedDetail>> details = new HashMap<Integer, List<AugmentedDetail>>();

            for (int i = 0; i < simpSteps.size(); i++) {
                Step step = simpSteps.get(i);

                String eventName = step.getMatchName().get(0).getActivityName();

                List<AugmentedDetail> stepdetails = transformToAugmentedDetails(eventName, step.getMatchDetails());

                query.appendToSequence(new Event(eventName, new TreeSet<AugmentedDetail>(stepdetails)));
                if (stepdetails.size() > 0)
                    details.put(i, stepdetails);
            }

            SequenceExplorer sqex = new SequenceExplorer(cluster, session, ks, cassandra_keyspace_name);
            Map<String, Map<String, Long>> followingEvents = sqex.exploreQueryAccurateStep(start_date, end_date, query, details, table_name, position, maxDuration);

            for (String event : followingEvents.keySet()) {
                if (!allTruePositives.containsKey(event))
                    allTruePositives.put(event, new HashMap<String, Long>());
                Long oldMaxDate = allTruePositives.get(event).get("maxDate");
                allTruePositives.get(event).putAll(followingEvents.get(event));
                if (oldMaxDate != null && oldMaxDate > allTruePositives.get(event).get("maxDate"))
                    allTruePositives.get(event).put("maxDate", oldMaxDate);
            }
        }

        List<Proposition> propositions = new ArrayList<Proposition>();

        for (Map.Entry<String, Map<String, Long>> entry : allTruePositives.entrySet()) {
            String event = entry.getKey();
            Map<String, Long> truePositives = entry.getValue();

            int count = truePositives.keySet().size() - 1;
            long totalDuration = 0;
            for (String sessionID : truePositives.keySet())
                if (!sessionID.equals("maxDate"))
                    totalDuration = totalDuration + truePositives.get(sessionID);

            Double avgDuration;
            String lastCompletedAt;
            if (count != 0) {
                avgDuration = (totalDuration / (truePositives.keySet().size() - 1)) / 1000.0;
                lastCompletedAt = Utilities.toISO_UTC(new Date(truePositives.get("maxDate")));
            } else {
                avgDuration = null;
                lastCompletedAt = null;
            }

            Proposition prop = new Proposition(event, count, avgDuration, lastCompletedAt);

            if (count != 0) // Only show interesting results
                propositions.add(prop);
        }
        Collections.sort(propositions);
        return propositions;
    }

    private List<Proposition> getExploreFast(List<Step> steps, Date start_date, Date end_date, int position, int lastCompletions) {
        List<List<Step>> listOfSteps = simplifySequences(steps);

        Map<String, Proposition> allPropositions = new HashMap<String, Proposition>();
        List<Proposition> results = Collections.emptyList();

        for (List<Step> simpList : listOfSteps) {
            Sequence query = new Sequence();
            Map<Integer, List<AugmentedDetail>> details = new HashMap<Integer, List<AugmentedDetail>>();

            for (int i = 0; i < simpList.size(); i++) {
                Step step = simpList.get(i);
                String eventName = step.getMatchName().get(0).getActivityName();

                List<AugmentedDetail> stepdetails = transformToAugmentedDetails(eventName, step.getMatchDetails());

                query.appendToSequence(new Event(eventName, new TreeSet<AugmentedDetail>(stepdetails)));
                if (stepdetails.size() > 0)
                    details.put(i, stepdetails);
            }

            SequenceExplorer sqex = new SequenceExplorer(cluster, session, ks, cassandra_keyspace_name);
            List<Proposition> followUps = sqex.exploreQueryFastStep(start_date, end_date, query, details, table_name, position, lastCompletions);

            for (Proposition prop : followUps) {
                String event = prop.getLogName();
                if (!allPropositions.containsKey(event))
                    allPropositions.put(event, new Proposition(event, 0));

                Proposition existingProp = allPropositions.get(event);
                existingProp.setCompletions(existingProp.getCompletions() + prop.getCompletions());
                if (existingProp.getCompletions() > lastCompletions)
                    existingProp.setCompletions(lastCompletions);
                if (existingProp.getAverageDuration() == null || prop.getAverageDuration() > existingProp.getAverageDuration())
                    existingProp.setAverageDuration(prop.getAverageDuration());
            }

            results = new ArrayList<>(allPropositions.values());

            Collections.sort(results);
        }

        return results;
    }

    private List<Proposition> getExploreHybrid(List<Step> steps, Date start_date, Date end_date, String table_name, int position, long maxDuration, int lastCompletions, int topK) {
        List<List<Step>> listOfSteps = simplifySequences(steps);

        List<Sequence> allQueries = new ArrayList<Sequence>();
        Map<Integer, List<AugmentedDetail>> details = Collections.emptyMap();

        for (List<Step> simpSteps : listOfSteps) {
            Sequence query = new Sequence();
            details = new HashMap<Integer, List<AugmentedDetail>>(); // WARNING: This actually gets rewritten in every loop but since all details are the same for each "simpSteps" list it doesn't matter

            for (int i = 0; i < simpSteps.size(); i++) {
                Step step = simpSteps.get(i);

                String eventName =step.getMatchName().get(0).getActivityName();

                List<AugmentedDetail> stepdetails = transformToAugmentedDetails(eventName, step.getMatchDetails());

                query.appendToSequence(new Event(eventName, new TreeSet<AugmentedDetail>(stepdetails)));
                if (stepdetails.size() > 0)
                    details.put(i, stepdetails);
            }

            allQueries.add(query);
        }

        List<Proposition> propositions = getExploreFast(steps, start_date, end_date, position, lastCompletions);

        List<Proposition> results = new ArrayList<Proposition>();

        int propCount = 0;

        Iterator<Proposition> it = propositions.iterator();
        while (it.hasNext()) {
            Proposition prop = it.next();

            if (propCount < topK) // Accurate evaluation
            {
                String event = prop.getLogName();
                Map<String, Long> allTruePositives = new TreeMap<String, Long>();

                for (Sequence query : allQueries) {
                    SequenceQueryEvaluator sqev = new SequenceQueryEvaluator(cluster, session, ks, cassandra_keyspace_name);

                    query.appendToSequence(new Event(event));

                    Set<String> candidates = sqev.evaluateQueryLogFile(start_date, end_date, query, details, table_name);
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
                if (count != 0) {
                    avgDuration = (totalDuration / (allTruePositives.keySet().size() - 1)) / 1000.0;
                    lastCompletedAt = Utilities.toISO_UTC(new Date(allTruePositives.get("maxDate")));
                } else {
                    avgDuration = null;
                    lastCompletedAt = null;
                }
                Proposition newprop = new Proposition(event, count, avgDuration, lastCompletedAt);

                if (count != 0) // Only show interesting results
                {
                    results.add(newprop);
                    propCount++;
                }
            } else
                results.add(prop);
        }

        return results;
    }


    /**
     * Simplify a list of steps potentially containing OR disjunctions between steps to a list of list of steps
     * that avoid OR disjunctions between their steps
     *
     * @param steps A list of the steps contained in the original funnel
     * @return A list of list of steps, each containing simple steps without logical operators between them
     */
    public List<List<Step>> simplifySequences(List<Step> steps) {
        List<List<Step>> allLists = new ArrayList<List<Step>>();

        Step firstStep = steps.get(0);

        // Start a new list for each Name in the first step of the funnel
        for (Name name : firstStep.getMatchName()) {
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
        for (int i = 1; i < steps.size(); i++) {
            List<List<Step>> tempAllLists = new ArrayList<List<Step>>();
            Step curStep = steps.get(i);
            for (Name name : curStep.getMatchName()) {
                for (List<Step> list : allLists) {
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
     *
     * @param eventName The event name of the AugmentedDetail
     * @param details   A list of details for this step
     * @return The corresponding list of AugmentedDetail
     */
    protected List<AugmentedDetail> transformToAugmentedDetails(String eventName, List<Detail> details) {
        List<AugmentedDetail> stepDetails = new ArrayList<AugmentedDetail>();

        if (details == null)
            return Collections.emptyList();

        for (int i = 0; i < details.size(); i++) {
            Detail detail = details.get(i);

            stepDetails.add(new AugmentedDetail(eventName, detail.getKey(), detail.getValue()));
        }
        return stepDetails;
    }
}
