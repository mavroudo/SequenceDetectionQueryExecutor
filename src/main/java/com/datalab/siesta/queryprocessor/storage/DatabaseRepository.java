package com.datalab.siesta.queryprocessor.storage;

import com.datalab.siesta.queryprocessor.model.DBModel.*;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.ExtractedPairsForPatternDetection;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface DatabaseRepository {

    Metadata getMetadata(String logname);

    Set<String> findAllLongNames();

    List<Count> getCounts(String logname, Set<EventPair> pairs);

    List<String> getEventNames(String logname);

    Map<Long,List<EventBoth>> querySeqTable(String logname, List<Long> traceIds, Set<String> eventTypes,Timestamp from, Timestamp till);

    Map<Long,List<EventBoth>> querySeqTable(String logname, List<Long> traceIds);

    IndexMiddleResult patterDetectionTraceIds(String logname, List<Tuple2<EventPair, Count>> combined, Metadata metadata, ExtractedPairsForPatternDetection pairs, Timestamp from, Timestamp till);

    IndexRecords queryIndexTable(Set<EventPair> pairs, String logname, Metadata metadata);
    IndexRecords queryIndexTable(Set<EventPair> pairs, String logname, Metadata metadata, Timestamp from, Timestamp till);

    List<EventBoth> querySingleTable(String logname, Set<Long> traceIds, Set<String> eventTypes);

    Map<Integer,List<EventBoth>> querySingleTableGroups(String logname, List<Set<Long>> groups, Set<String> eventTypes);

    List<Count> getCountForExploration(String logname, String event);

}
