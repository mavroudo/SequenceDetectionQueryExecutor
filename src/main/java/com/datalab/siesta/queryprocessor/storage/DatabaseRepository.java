package com.datalab.siesta.queryprocessor.storage;

import com.datalab.siesta.queryprocessor.declare.model.EventPairToTrace;
import com.datalab.siesta.queryprocessor.declare.model.EventSupport;
import com.datalab.siesta.queryprocessor.declare.model.UniqueTracesPerEventPair;
import com.datalab.siesta.queryprocessor.declare.model.UniqueTracesPerEventType;
import com.datalab.siesta.queryprocessor.declare.model.declareState.ExistenceState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.NegativeState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.OrderState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.PositionState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.UnorderStateI;
import com.datalab.siesta.queryprocessor.declare.model.declareState.UnorderStateU;
import com.datalab.siesta.queryprocessor.model.DBModel.*;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.ExtractedPairsForPatternDetection;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import scala.Tuple3;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interface that contains the signature of methods that needs to be implemented by each class in order to
 * be utilized in the SIESTA query response
 */
public interface DatabaseRepository {

    /**
     *
     * @param logname the log database
     * @return a list with all the event types stored in it
     */
    Metadata getMetadata(String logname);

    /**
     * @return a set with all the stored log databases
     */
    Set<String> findAllLongNames();


    /**
     * Retrieves the corresponding stats (min, max duration and so on) from the CountTable, for a given set of event
     * pairs
     * @param logname the log database
     * @param pairs a set with the event pairs
     * @return a list of the stats for the set of event pairs
     */
    List<Count> getCounts(String logname, Set<EventPair> pairs);

    /**
     * Retrieves the event pairs tha appear as keys in the CountTable
     * @param logname the log database
     * @return a list of event pairs
     */
    List<Count> getEventPairs(String logname);

    /**
     *
     * @param logname the log database
     * @return a list with all the event types stored in it
     */
    List<String> getEventNames(String logname);

    /**
     * Retrieves the appropriate events from the SequenceTable, which contains the original traces
     * @param logname the log database
     * @param traceIds the ids of the traces that will be retrieved
     * @param eventTypes the events that will be retrieved
     * @param from the starting timestamp, set to null if not used
     * @param till the ending timestamp, set to null if not used
     * @return a map where the key is the trace id and the value is a list of the retrieved events (with their
     * timestamps)
     */
    Map<String,List<EventBoth>> querySeqTable(String logname, List<String> traceIds, Set<String> eventTypes,Timestamp from, Timestamp till);

    /**
     * Retrieves the appropriate events from the SequenceTable, which contains the original traces
     * @param logname the log database
     * @param traceIds the ids of the traces that will be retrieved
     * @return a map where the key is the trace id and the value is a list of the retrieved events (with their
     *      * timestamps)
     */
    Map<String,List<EventBoth>> querySeqTable(String logname, List<String> traceIds);

    /**
     * Detects the traces that contain all the given event pairs
     * @param logname the log database
     * @param combined a list where each event pair is combined with the according stats from the CountTable
     * @param metadata the log database metadata
     * @param pairs the event pairs extracted from the query
     * @param from the starting timestamp, set to null if not used
     * @param till the ending timestamp, set to null if not used
     * @return the traces that contain all the pairs. It will be then processed by SASE in order to remove false
     * positives.
     */
    IndexMiddleResult patterDetectionTraceIds(String logname, List<Tuple2<EventPair, Count>> combined, Metadata metadata, ExtractedPairsForPatternDetection pairs, Timestamp from, Timestamp till);

    /**
     * Retrieves data from the primary inverted index
     * @param pairs a set of the pairs that we need to retrieve information for
     * @param logname the log database
     * @return the corresponding records from the index
     */
    IndexRecords queryIndexTable(Set<EventPair> pairs, String logname);

   /**
     * Retrieves data from the primary inverted index
     * @param pairs a set of the pairs that we need to retrieve information for
     * @param logname the log database
     * @param metadata the metadata for this log database
     * @param from the starting timestamp, set to null if not used
     * @param till the ending timestamp, set to null if not used
     * @return the corresponding records from the index
     */
    IndexRecords queryIndexTable(Set<EventPair> pairs, String logname, Metadata metadata, Timestamp from, Timestamp till);

    /**
     * Retrieves the appropriate events from the SingleTable, which contains the single inverted index
     * @param logname the log database
     * @param traceIds the ids of the traces that wil be retrieved
     * @param eventTypes the events that will we retrieved
     * @return a list of all the retrieved events (with their timestamps)
     */
    List<EventBoth> querySingleTable(String logname, Set<String> traceIds, Set<String> eventTypes);

    /**
     * Retrieves the appropriate events from the SingleTable, which contains the single inverted index
     * @param logname the log database
     * @param eventTypes the events that will we retrieved
     * @return a map of all the retrieved events (with their timestamps) grouped by the traceID
     */
    Map<String,List<EventBoth>> querySingleTable(String logname, Set<String> eventTypes);


    /**
     * Retrieves the appropriate events from the SingleTable, which contains the single inverted index
     * @param logname the log database
     * @param groups a list of the groups as defined in the query
     * @param eventTypes the events that will we retrieved
     * @return a map where the key is the group id and the value is a list of the retrieved events (with their t
     * imestamps)
     */
    Map<Integer,List<EventBoth>> querySingleTableGroups(String logname, List<Set<String>> groups, Set<String> eventTypes);

    /**
     * For a given event type inside a log database, returns all the possible next events. That is, since Count
     * contains for each pair the stats, return all the events that have at least one pair with the given event
     * @param logname the log database
     * @param event the event type
     * @return the possible next events
     */
    List<Count> getCountForExploration(String logname, String event);


    // Below are for Declare //
    JavaRDD<Trace> querySequenceTableDeclare(String logname);

    JavaRDD<UniqueTracesPerEventType> querySingleTableDeclare(String logname);

    JavaRDD<EventSupport> querySingleTable(String logname);

    JavaRDD<UniqueTracesPerEventPair> queryIndexTableDeclare(String logname);

    JavaRDD<IndexPair> queryIndexTableAllDeclare(String logname);

    JavaPairRDD<Tuple2<String,String>, List<Integer>> querySingleTableAllDeclare(String logname);

    JavaRDD<EventPairToTrace> queryIndexOriginalDeclare(String logname);

    //Below are for the states of Declare
    JavaRDD<PositionState> queryPositionState(String logname);
    JavaRDD<ExistenceState> queryExistenceState(String logname);
    JavaRDD<UnorderStateI> queryUnorderStateI(String logname);
    JavaRDD<UnorderStateU> queryUnorderStateU(String logname);
    JavaRDD<OrderState> queryOrderState(String logname);
    JavaRDD<NegativeState> queryNegativeState(String logname);


}
