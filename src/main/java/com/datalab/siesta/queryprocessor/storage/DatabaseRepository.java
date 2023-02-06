package com.datalab.siesta.queryprocessor.storage;

import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraintWE;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraintWE;
import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexMiddleResult;
import com.datalab.siesta.queryprocessor.model.EventPair;
import com.datalab.siesta.queryprocessor.model.Metadata;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface DatabaseRepository {

    Metadata getMetadata(String logname);

    Set<String> findAllLongNames();

    List<Count> getCounts(String logname, Set<EventPair> pairs);

    List<String> getEventNames(String logname);

    IndexMiddleResult patterDetectionTraceIds(String logname, List<Tuple2<EventPair, Count>> combined,Metadata metadata);

}
