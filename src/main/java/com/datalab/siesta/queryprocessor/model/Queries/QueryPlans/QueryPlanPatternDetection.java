package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans;

import com.datalab.siesta.queryprocessor.Application;
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

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueryPlanPatternDetection implements QueryPlan {

    protected final DBConnector dbConnector;

    protected Metadata metadata;

    protected int minPairs;

    protected final SaseConnector saseConnector;

    protected IndexMiddleResult imr;

    protected Utils utils;

    protected Set<String> eventTypesInLog;

    public void setEventTypesInLog(Set<String> eventTypesInLog) {
        this.eventTypesInLog = eventTypesInLog;
    }

    public IndexMiddleResult getImr() { //no need for this is just for testing
        return imr;
    }

    @Autowired
    public QueryPlanPatternDetection(DBConnector dbConnector, SaseConnector saseConnector, Utils utils) {
        this.saseConnector = saseConnector;
        this.dbConnector = dbConnector;
        this.utils = utils;
        minPairs = -1;
    }

    public void setMinPairs(int minPairs) {
        this.minPairs = minPairs;
    }

    @Override
    public QueryResponse execute(QueryWrapper qw) {
        long start = System.currentTimeMillis();
        QueryPatternDetectionWrapper qpdw = (QueryPatternDetectionWrapper) qw;
        QueryResponseBadRequestForDetection firstCheck = new QueryResponseBadRequestForDetection();
        this.getMiddleResults(qpdw,firstCheck);
        Logger logger = LoggerFactory.getLogger(Application.class);
        logger.info(String.format("Retrieve event pairs: %d ms",System.currentTimeMillis()-start));
        if(!firstCheck.isEmpty()) return firstCheck; //stop the process as an error was found
        QueryResponsePatternDetection queryResponsePatternDetection = new QueryResponsePatternDetection();
        checkIfRequiresDataFromSequenceTable(qpdw); //check if data is required from the sequence table and gets them
        long ts_trace = System.currentTimeMillis();
        List<Occurrences> occurrences = saseConnector.evaluate(qpdw.getPattern(), imr.getEvents(), false);
        occurrences.forEach(x->x.clearOccurrences(qpdw.isReturnAll()));
        long ts_eval = System.currentTimeMillis();
        queryResponsePatternDetection.setOccurrences(occurrences);
        TimeStats timeStats = new TimeStats();
        timeStats.setTimeForPrune(ts_trace-start);
        timeStats.setTimeForValidation(ts_eval-ts_trace);
        timeStats.setTotalTime(ts_eval-start);
        queryResponsePatternDetection.setTimeStats(timeStats);
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
     * @param qpdw the user query
     * @param qr a wrapper that contains probable inconsistencies in the query
     */
    protected void getMiddleResults(QueryPatternDetectionWrapper qpdw, QueryResponseBadRequestForDetection qr){
        boolean fromOrTillSet = qpdw.getFrom()!=null || qpdw.getTill()!=null;
        ExtractedPairsForPatternDetection pairs = qpdw.getPattern().extractPairsForPatternDetection(fromOrTillSet);
        List<Count> sortedPairs = this.getStats(pairs.getAllPairs(), qpdw.getLog_name());
        List<Tuple2<EventPair, Count>> combined = this.combineWithPairs(pairs.getAllPairs(), sortedPairs);
        qr = this.firstParsing(qpdw, pairs.getAllPairs(), combined,qr); // checks if all are correctly set before start querying
        if (!qr.isEmpty()) return; //There was an original error
        imr = dbConnector.patterDetectionTraceIds(qpdw.getLog_name(), combined, metadata, pairs,qpdw.getFrom(),qpdw.getTill());
    }


    protected boolean requiresQueryToDB(QueryPatternDetectionWrapper qpdw) {
        Tuple2<List<TimeConstraint>, List<GapConstraint>> cl= utils.splitConstraints(qpdw.getPattern().getConstraints());
        return (!cl._1.isEmpty() && !cl._2.isEmpty()) || (!cl._2.isEmpty() && metadata.getMode().equals("timestamps")) ||
                (!cl._1.isEmpty() && metadata.getMode().equals("positions")) ||
                (qpdw.getFrom()!=null && metadata.getMode().equals("positions")) || //considering also from and till info
                (qpdw.getTill()!=null && metadata.getMode().equals("positions"));
    }

    protected void checkIfRequiresDataFromSequenceTable(QueryPatternDetectionWrapper qpdw){
        long start = System.currentTimeMillis();
        if (this.requiresQueryToDB(qpdw)) { // we need to get from SeqTable
            //we first run a quick sase engine to remove all possible mismatches, and then we query the seq for the rest
            List<Occurrences> ocs = saseConnector.evaluate(qpdw.getPattern(), imr.getEvents(), true);
            List<Long> tracesToQuery = ocs.stream().map(Occurrences::getTraceID).collect(Collectors.toList());
            Map<Long,List<Event>> e = this.querySeqDB(tracesToQuery, qpdw.getPattern(), qpdw.getLog_name(),qpdw.getFrom(),qpdw.getTill());
            imr.setEvents(e);
            Logger logger = LoggerFactory.getLogger(Application.class);
            logger.info(String.format("Retrieve traces: %d ms",System.currentTimeMillis()-start));
        }
    }

    protected Map<Long, List<Event>> querySeqDB(List<Long> trace_ids, SIESTAPattern pattern, String logname, Timestamp from, Timestamp till){
        Set<String> eventTypes = pattern.getEventTypes();
        Map<Long, List<EventBoth>> fromDB = dbConnector.querySeqTable(logname, trace_ids, eventTypes,from,till);
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

    protected List<Tuple2<EventPair, Count>> combineWithPairs(Set<EventPair> pairs, List<Count> sortedCounts) {
        List<Tuple2<EventPair, Count>> response = new ArrayList<>();
        for (Count c : sortedCounts) {
            for (EventPair p : pairs) {
                if (p.equals(c)) {
                    response.add(new Tuple2<>(p, c));
                }
            }
        }
        return response;
    }

    protected QueryResponseBadRequestForDetection firstParsing(QueryPatternDetectionWrapper queryPatternDetectionWrapper,
                                                               Set<EventPair> pairs,
                                                               List<Tuple2<EventPair, Count>> combined,
                                                               QueryResponseBadRequestForDetection qr) {
//        QueryResponseBadRequestForDetection qr = new QueryResponseBadRequestForDetection();
        List<String> nonExistingEvents = new ArrayList<>();
        for (String eventType : queryPatternDetectionWrapper.getPattern().getEventTypes()) {
            if (!this.eventTypesInLog.contains(eventType)) {
                nonExistingEvents.add(eventType);
            }
        }
        if (!nonExistingEvents.isEmpty()) {
            qr.setNonExistingEvents(nonExistingEvents);
        }
        List<Constraint> wrongConstraints = new ArrayList<>();
        for(Constraint c: queryPatternDetectionWrapper.getPattern().getConstraints()){
            if (c.hasError()) wrongConstraints.add(c);
        }
        if(!wrongConstraints.isEmpty()){
            qr.setWrongConstraints(wrongConstraints);
        }


        List<EventPair> inPairs = new ArrayList<>();
        List<EventPair> fromCombined = combined.stream().map(x->x._1).collect(Collectors.toList());
        if (pairs.size() != combined.size()) { //find non-existing event pairs
            for (EventPair pair : pairs) {
                if (!fromCombined.contains(pair)) {
                    inPairs.add(pair);
                }
            }
            if (inPairs.size() > 0) {
                qr.setNonExistingPairs(inPairs);
            }
        }
        for (Tuple2<EventPair, Count> c : combined) { //Find constraints that do not hold in all db
            if (c._1.getConstraint() instanceof TimeConstraint) {
                TimeConstraint tc = (TimeConstraint) c._1.getConstraint();
                if (!tc.isConstraintHolds(c._2)) {
                    inPairs.add(c._1);
                }
            }
        }
        if (inPairs.size() > 0) {
            qr.setConstraintsNotFulfilled(inPairs);
        }
        return qr;

    }

}
