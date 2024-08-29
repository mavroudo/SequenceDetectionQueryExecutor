package com.datalab.siesta.queryprocessor.storage;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexMiddleResult;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.ExtractedPairsForPatternDetection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.*;

/**
 * This class is the middle man between the databases and the rest of the application. It provides methods to do certain
 * operations in the database utilizing the methods described in the DatabaseRepository interface.
 */
@Service
public class DBConnector {

    /**
     * Database object. Depending on the resources it is either implements the connection with S3 or the connection
     * with Cassandra.
     */
    private DatabaseRepository db;

    @Autowired
    public DBConnector(DatabaseRepository databaseRepository) {
        this.db = databaseRepository;
    }

    /**
     * @param logname the log database
     * @return the metadata
     */
    public Metadata getMetadata(String logname) {
        return db.getMetadata(logname);
    }

    /**
     * @param logname the log database
     * @return a list with all the event types stored in it
     */
    public List<String> getEventNames(String logname) {
        return db.getEventNames(logname);
    }

    /**
     * @return a set with all the stored log databases
     */
    public Set<String> findAllLongNames() {
        return db.findAllLongNames();
    }

    /**
     * Retrieves the corresponding stats (min, max duration and so on) from the CountTable, for a given set of event
     * pairs
     *
     * @param logname    the log database
     * @param eventPairs a set with the event pairs
     * @return a list of the stats for the set of event pairs
     */
    public List<Count> getStats(String logname, Set<EventPair> eventPairs) {
        return db.getCounts(logname, eventPairs);
    }

    /**
     * For a given event type inside a log database, returns all the possible next events. That is, since Count
     * contains for each pair the stats, return all the events that have at least one pair with the given event
     *
     * @param logname the log database
     * @param event   the event type
     * @return the possible next events
     */
    public List<Count> getCountForExploration(String logname, String event) {
        return db.getCountForExploration(logname, event);
    }

    /**
     * For a given log database, returns all the event pairs found in the log
     *
     * @param logname the log database
     * @return all event pairs found in the log
     */
    public List<Count> getEventPairs(String logname) {
        return db.getEventPairs(logname);
    }

    /**
     * Detects the traces that contain all the given event pairs
     *
     * @param logname  the log database
     * @param combined a list where each event pair is combined with the according stats from the CountTable
     * @param metadata the log database metadata
     * @param pairs    the event pairs extracted from the query
     * @param from     the starting timestamp, set to null if not used
     * @param till     the ending timestamp, set to null if not used
     * @return the traces that contain all the pairs. It will be then processed by SASE in order to remove false
     * positives.
     */
    public IndexMiddleResult patterDetectionTraceIds(String logname, List<Tuple2<EventPair, Count>> combined, Metadata metadata, ExtractedPairsForPatternDetection pairs, Timestamp from, Timestamp till) {
        return db.patterDetectionTraceIds(logname, combined, metadata, pairs, from, till);
    }

    /**
     * Retrieves the appropriate events from the SequenceTable, which contains the original traces
     *
     * @param logname    the log database
     * @param traceIds   the ids of the traces that will be retrieved
     * @param eventTypes the events that will be retrieved
     * @param from       the starting timestamp, set to null if not used
     * @param till       the ending timestamp, set to null if not used
     * @return a map where the key is the trace id and the value is a list of the retrieved events (with their
     * timestamps)
     */
    public Map<String, List<EventBoth>> querySeqTable(String logname, List<String> traceIds, Set<String> eventTypes, Timestamp from, Timestamp till) {
        return db.querySeqTable(logname, traceIds, eventTypes, from, till);
    }

    /**
     * Retrieves all the events from specific traces in the SequenceTable
     * @param logname the log database
     * @param traceIds the ids of the traces that will be retrieved
     * @return a map where the key is the trace id and the value is a list of the retrieved events (with their
     *      * timestamps)
     */
    public Map<String, List<EventBoth>> querySeqTable(String logname, List<String> traceIds) {
        return db.querySeqTable(logname, traceIds);
    }

    /**
     * Retrieves the appropriate events from the SingleTable, which contains the single inverted index
     *
     * @param logname    the log database
     * @param traceIds   the ids of the traces that wil be retrieved
     * @param eventTypes the events that will we retrieved
     * @return a list of all the retrieved events (wth their timestamps)
     */
    public List<EventBoth> querySingleTable(String logname, Set<String> traceIds, Set<String> eventTypes) {
        return db.querySingleTable(logname, traceIds, eventTypes);
    }

    /**
     * Retrieves event from the SingleTable and group them based on the traceID
     * @param logname the log database
     * @param eventTypes the eventTypes that we want to retrieve
     * @return a Map of the ids of the traces and the events that belong to this
     */
    public Map<String,List<EventBoth>> getEventsFromSingleTableGroupedByTraceID(String logname, Set<String> eventTypes){
        Map<String,List<EventBoth>> retrieved = db.querySingleTable(logname,eventTypes);
        Map<String,List<EventBoth>> answer = new HashMap<>();
        retrieved.forEach((name,eventList)->{
            for(EventBoth e: eventList){
                if(answer.containsKey(e.getTraceID())){
                    answer.get(e.getTraceID()).add(e);
                }else{
                    List<EventBoth> tempList = new ArrayList<>();
                    tempList.add(e);
                    answer.put(e.getTraceID(),tempList);
                }
            }

        });
        Comparator<EventBoth> comparator = Comparator.comparing(EventBoth::getTimestamp);
        for (List<EventBoth> list : answer.values()) {
            list.sort(comparator);
        }
        return answer;
    }

    /**
     * Retrieves the appropriate events from the SingleTable, which contains the single inverted index
     *
     * @param logname    the log database
     * @param groups     a list of the groups as defined in the query
     * @param eventTypes the events that will we retrieved
     * @return a map where the key is the group id and the value is a list of the retrieved events (with their t
     * imestamps)
     */
    public Map<Integer, List<EventBoth>> querySingleTableGroups(String logname, List<Set<String>> groups, Set<String> eventTypes) {
        return db.querySingleTableGroups(logname, groups, eventTypes);
    }


}
