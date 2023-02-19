package com.datalab.siesta.queryprocessor.storage;

import com.datalab.siesta.queryprocessor.model.DBModel.*;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface DatabaseRepository {

    Metadata getMetadata(String logname);

    Set<String> findAllLongNames();

    List<Count> getCounts(String logname, Set<EventPair> pairs);

    List<String> getEventNames(String logname);

    Map<Long,List<EventBoth>> querySeqTable(String logname, List<Long> traceIds, Set<String> eventTypes);

    Map<Long,List<EventBoth>> querySeqTable(String logname, List<Long> traceIds);

    IndexMiddleResult patterDetectionTraceIds(String logname, List<Tuple2<EventPair, Count>> combined,Metadata metadata, int minPairs);

    IndexRecords queryIndexTable(Set<EventPair> pairs, String logname, Metadata metadata);

}
