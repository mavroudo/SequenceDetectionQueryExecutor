package com.datalab.siesta.queryprocessor.storage;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexMiddleResult;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class DBConnector {

    private DatabaseRepository db;

    @Autowired
    public DBConnector(DatabaseRepository databaseRepository) {
        this.db = databaseRepository;
    }

    public Metadata getMetadata(String logname) {
        return db.getMetadata(logname);
    }

    public List<String> getEventNames(String logname){ return db.getEventNames(logname);}

    public Set<String> findAllLongNames() {
        return db.findAllLongNames();
    }

    public List<Count> getStats(String logname, Set<EventPair> eventPairs) {
        return db.getCounts(logname, eventPairs);
    }

    public IndexMiddleResult patterDetectionTraceIds(String logname, List<Tuple2<EventPair, Count>> combined,Metadata metadata, int minpairs){
        return db.patterDetectionTraceIds(logname,combined,metadata,minpairs);
    }

    public Map<Long,List<EventBoth>> querySeqTable(String logname, List<Long> traceIds, Set<String> eventTypes){
        return db.querySeqTable(logname,traceIds,eventTypes);
    }

    public List<EventBoth> querySingleTable(String logname, Set<Long> traceIds, Set<String> eventTypes){
        return db.querySingleTable(logname,traceIds,eventTypes);
    }

    public Map<Integer,List<EventBoth>> querySingleTableGroups(String logname, List<Set<Long>> groups, Set<String> eventTypes){
        return db.querySingleTableGroups(logname,groups,eventTypes);
    }
}
