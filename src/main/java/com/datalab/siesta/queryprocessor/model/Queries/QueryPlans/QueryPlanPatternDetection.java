package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseConnector;
import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraint;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraint;
import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexMiddleResult;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.ExtractedPairsForPatternDetection;
import com.datalab.siesta.queryprocessor.model.Occurrences;
import com.datalab.siesta.queryprocessor.model.Patterns.SIESTAPattern;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseBadRequestForDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponsePatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.model.TimeStats;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

/**
 * The query plan of the pattern detection query
 */
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueryPlanPatternDetection implements QueryPlan {

    /**
     * Connection with the database
     */
    protected final DBConnector dbConnector;

    /**
     * Log Database's metadata
     */
    protected Metadata metadata;

    /**
     * Connection with the SASE
     */
    protected final SaseConnector saseConnector;

    /**
     * IntermediateResults: Holds the traces that contain all the required et-pairs.
     * These are the results before the validation step that will remove the false positives
     */
    protected IndexMiddleResult imr;

    protected Utils utils;

    /**
     * A set of all the event types in this log database.
     */
    protected Set<String> eventTypesInLog;

    public void setEventTypesInLog(Set<String> eventTypesInLog) {
        this.eventTypesInLog = eventTypesInLog;
    }

    public IndexMiddleResult getImr() { //no need for this is just for testing
        return imr;
    }

    /**
     * Using Spring Boot framework to get the required objects from the Heap. These are the connection to the database,
     * the connection to Sase and the utilities
     *
     * @param dbConnector   Connection with the database
     * @param saseConnector Connection with the SASE
     * @param utils         utilities object
     */
    @Autowired
    public QueryPlanPatternDetection(DBConnector dbConnector, SaseConnector saseConnector, Utils utils) {
        this.saseConnector = saseConnector;
        this.dbConnector = dbConnector;
        this.utils = utils;
    }

    /**
     * The query plan has the following steps:
     * (1) Retrieve the required et-pairs from the database and prune the search space
     * (2) Validate the remaining pairs to remove false positives.
     * The (1) is handled inside the getMiddleResults function while the (2) is handled using the saseConnector
     *
     * @param qw the QueryPatternDetectionWrapper
     * @return a QueryResponsePatternDetection if succeed or a QueryResponseBadRequestForDetection if failed
     */
    @Override
    public QueryResponse execute(QueryWrapper qw) {
        long start = System.currentTimeMillis();
        QueryPatternDetectionWrapper qpdw = (QueryPatternDetectionWrapper) qw;
        QueryResponseBadRequestForDetection firstCheck = new QueryResponseBadRequestForDetection();
        this.getMiddleResults(qpdw, firstCheck);
        Logger logger = LoggerFactory.getLogger(QueryPlanPatternDetection.class);
        logger.info(String.format("Retrieve event pairs: %d ms", System.currentTimeMillis() - start));
        if (!firstCheck.isEmpty()) return firstCheck; //stop the process as an error was found
        QueryResponsePatternDetection queryResponsePatternDetection = new QueryResponsePatternDetection();
        //check if data is required from the sequence table and gets them
        if (this.requiresQueryToDB(qpdw)) { // we need to get from SeqTable
            retrieveTimeInformation(qpdw.getPattern(), qpdw.getLog_name(), qpdw.getFrom(), qpdw.getTill());
        }



        long ts_trace = System.currentTimeMillis();
        List<Occurrences> occurrences = saseConnector.evaluate(qpdw.getPattern(), imr.getEvents(), false);
        occurrences.forEach(x -> x.clearOccurrences(qpdw.isReturnAll()));
        long ts_eval = System.currentTimeMillis();
        queryResponsePatternDetection.setOccurrences(occurrences);
        TimeStats timeStats = new TimeStats(); //create the response stats (pruning,validation and total time)
        timeStats.setTimeForPrune(ts_trace - start);
        timeStats.setTimeForValidation(ts_eval - ts_trace);
        timeStats.setTotalTime(ts_eval - start);
        queryResponsePatternDetection.setTimeStats(timeStats); //add time-stats to the response object
        return queryResponsePatternDetection;
    }

    /**
     * Calculates the middle results of the pattern detection. That is, for a given query, evaluates if the query can be
     * executed (all the event types and event pairs exist at least once in the indices). If the query is valid then
     * it proceeds, otherwise it returns.
     * Check if the qr is empty before proceeding. If it is empty it means that everything is ok, otherwise it means
     * that the query is not consistent and did not move further.
     * If the query actually executed then it used the method patternDetectiontraceIds from the dbConnector which finds
     * all the traces that contains all the appropriate pairs. The traces are then stored in the IndexMiddleResult
     * so that they are available in the rest of the methods
     *
     * @param qpdw the user query
     * @param qr   a wrapper that contains probable inconsistencies in the query
     */
    protected void getMiddleResults(QueryPatternDetectionWrapper qpdw, QueryResponseBadRequestForDetection qr) {
        boolean fromOrTillSet = qpdw.getFrom() != null || qpdw.getTill() != null;
        ExtractedPairsForPatternDetection pairs = qpdw.getPattern().extractPairsForPatternDetection(fromOrTillSet);
        List<Count> sortedPairs = this.getStats(pairs.getAllPairs(), qpdw.getLog_name());
        List<Tuple2<EventPair, Count>> combined = this.combineWithPairs(pairs.getAllPairs(), sortedPairs);

        //check if the true pairs, constraints and event types are set correctly before start querying
        List<Count> sortedTruePairs = filterTruePairs(sortedPairs, pairs.getTruePairs());
        List<Tuple2<EventPair, Count>> combinedTrue = this.combineWithPairs(pairs.getTruePairs(), sortedTruePairs);
        qr = this.firstParsing(qpdw, pairs.getTruePairs(), combinedTrue, qr);

        if (!qr.isEmpty()) return; //There was an original error
        imr = dbConnector.patterDetectionTraceIds(qpdw.getLog_name(), combined, metadata, pairs, qpdw.getFrom(), qpdw.getTill());
    }

    /**
     * Filters the true pairs, so they can be passed to the first evaluation
     *
     * @param sortedPairs the ordered stats for the et-pairs retrieved from the CountTable
     * @param truePairs   the true et-pairs
     * @return the ordered stats only for the true pairs
     */
    protected List<Count> filterTruePairs(List<Count> sortedPairs, Set<EventPair> truePairs) {
        return sortedPairs.stream().filter(x -> truePairs.contains(
                        new EventPair(new Event(x.getEventA()), new Event(x.getEventB()))))
                .collect(Collectors.toList());
    }


    /**
     * Check if the query pattern requires to get information from the SequenceTable. That is, if the query describes a
     * time constraint and the IndexTable was build using positions, and vice versa, information is required that are
     * only stored in the SequenceTable.
     *
     * @param qpdw the query pattern
     * @return if it is required to get information from the sequence table, or if the available information in IndexTable
     * is adequate.
     */
    protected boolean requiresQueryToDB(QueryPatternDetectionWrapper qpdw) {
        Tuple2<List<TimeConstraint>, List<GapConstraint>> cl = utils.splitConstraints(qpdw.getPattern().getConstraints());
        return (!cl._1.isEmpty() && !cl._2.isEmpty()) || (!cl._2.isEmpty() && metadata.getMode().equals("timestamps")) ||
                (!cl._1.isEmpty() && metadata.getMode().equals("positions")) ||
                (qpdw.getFrom() != null && metadata.getMode().equals("positions")) || //considering also from and till info
                (qpdw.getTill() != null && metadata.getMode().equals("positions"));
    }


    /**
     * It retrieves the events, from the SequenceTable, and store them in the IndexMiddleResults object (overriding the previous ones).
     * That is ok, because the events from SequenceTable will always contain both time and position information
     *
     * @param pattern the query pattern
     * @param logname the name of log database
     * @param from starting timestamp (set to null if not used)
     * @param till ending timestamp (set to null if not used)
     */
    protected void retrieveTimeInformation(SIESTAPattern pattern, String logname, Timestamp from, Timestamp till) {
        //we first run a quick sase engine to remove all possible mismatches, and then we query the seq for the rest
        long start = System.currentTimeMillis();
        List<Occurrences> ocs = saseConnector.evaluate(pattern, imr.getEvents(), true);
        List<Long> tracesToQuery = ocs.stream().map(Occurrences::getTraceID).collect(Collectors.toList());
        Map<Long, List<Event>> e = this.querySeqDB(tracesToQuery, pattern, logname, from, till);
        imr.setEvents(e);
        Logger logger = LoggerFactory.getLogger(QueryPlanPatternDetection.class);
        logger.info(String.format("Retrieve traces: %d ms", System.currentTimeMillis() - start));
    }

    /**
     * Retrieves events from the SequenceTable
     *
     * @param trace_ids a list of all the trace ids
     * @param pattern   the query pattern (used to get the list of all the events)
     * @param logname   the log database
     * @param from      the starting timestamp
     * @param till      the ending timestamp
     * @return the retrieved events from the SequenceTable
     */
    protected Map<Long, List<Event>> querySeqDB(List<Long> trace_ids, SIESTAPattern pattern, String logname, Timestamp from, Timestamp till) {
        Set<String> eventTypes = pattern.getEventTypes();
        Map<Long, List<EventBoth>> fromDB = dbConnector.querySeqTable(logname, trace_ids, eventTypes, from, till);
        return fromDB.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().stream().map(s -> (Event) s)
                .collect(Collectors.toList())));
    }


    @Override
    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }


    /**
     * Query for the stats for the different pairs in the CountTable. Once get the results will sort them based
     * on their frequency and return the results.
     *
     * @param pairs   The event pairs generated
     * @param logname The name of the log file that we are searching in
     * @return The stats for the event pairs sorted by their frequency (This can be easily changed)
     */
    protected List<Count> getStats(Set<EventPair> pairs, String logname) {
        List<Count> results = dbConnector.getStats(logname, pairs);
        results.sort(Comparator.comparingInt(Count::getCount)); // can separate this function in order to be easily changed
        return results;
    }

    /**
     * Combines the pairs with the ordered retrieved stats from the CountTable
     *
     * @param pairs        the et-pairs
     * @param sortedCounts the ordered stats for the et-pairs retrieved from the CountTable
     * @return the combined list
     */
    protected List<Tuple2<EventPair, Count>> combineWithPairs(Set<EventPair> pairs, List<Count> sortedCounts) {
        List<Tuple2<EventPair, Count>> response = new ArrayList<>();
        for (Count c : sortedCounts) {
            for (EventPair p : pairs) {
                if (p.equals(c)) { //a correlation between counts and event pairs is already described in the equals
                    // function of the EventPair class
                    response.add(new Tuple2<>(p, c));
                }
            }
        }
        return response;
    }

    /**
     * This function evaluates the query pattern. Based on the retrieved stats for each consecutive et-pair checks if the
     * query fulfills the following conditions:
     * (1) All the events exist in the database
     * (2) No constraint contains error
     * (3) All the et-pairs exist in the database
     * (4) All conditions (pos and time) can be fulfilled by at least one event-pair in the database
     * If the query meets all conditions the QueryResponseBadRequestForDetection will remain empty, else for each
     * violation and for each condition relevant information will be added to the response
     *
     * @param queryPatternDetectionWrapper the query pattern
     * @param pairs                        the extracted pairs
     * @param combined                     the above pairs combined with the stats retrieved from the CountTable
     * @param qr                           a response that will store all the violations of the
     * @return the response object
     */
    protected QueryResponseBadRequestForDetection firstParsing(QueryPatternDetectionWrapper queryPatternDetectionWrapper,
                                                               Set<EventPair> pairs,
                                                               List<Tuple2<EventPair, Count>> combined,
                                                               QueryResponseBadRequestForDetection qr) {
        // check if all events exist in the database
        List<String> nonExistingEvents = new ArrayList<>();
        for (String eventType : queryPatternDetectionWrapper.getPattern().getEventTypes()) {
            if (!this.eventTypesInLog.contains(eventType)) {
                nonExistingEvents.add(eventType);
            }
        }
        if (!nonExistingEvents.isEmpty()) {
            qr.setNonExistingEvents(nonExistingEvents);
        }

        //check if all constraints are ok
        List<Constraint> wrongConstraints = new ArrayList<>();
        for (Constraint c : queryPatternDetectionWrapper.getPattern().getConstraints()) {
            if (c.hasError()) wrongConstraints.add(c);
        }
        if (!wrongConstraints.isEmpty()) {
            qr.setWrongConstraints(wrongConstraints);
        }

        //check if all pairs exist in the database
        List<EventPair> inPairs = new ArrayList<>();
        List<EventPair> fromCombined = combined.stream().map(x -> x._1).collect(Collectors.toList());
        if (pairs.size() != combined.size()) { //find non-existing event pairs
            for (EventPair pair : pairs) {
                if (!fromCombined.contains(pair)) {
                    inPairs.add(pair);
                }
            }
            if (!inPairs.isEmpty()) {
                qr.setNonExistingPairs(inPairs);
            }
        }

        //Find constraints that do not hold in all db
        List<EventPair> cannotBeFullFilled = new ArrayList<>();
        for (Tuple2<EventPair, Count> c : combined) {
            if (c._1.getConstraint() == null) {
                continue;
            }
            if (c._1.getConstraint() instanceof TimeConstraint) {
                TimeConstraint tc = (TimeConstraint) c._1.getConstraint();
                if (!tc.isConstraintHolds(c._2)) {
                    cannotBeFullFilled.add(c._1);
                }
            }
        }
        if (!cannotBeFullFilled.isEmpty()) {
            qr.setConstraintsNotFulfilled(cannotBeFullFilled);
        }
        return qr;

    }

}
