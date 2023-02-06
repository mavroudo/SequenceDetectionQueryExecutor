package com.datalab.siesta.queryprocessor.storage.repositories;

import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraint;
import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraintWE;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraint;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraintWE;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexPair;
import com.datalab.siesta.queryprocessor.model.EventPair;
import com.datalab.siesta.queryprocessor.model.Metadata;
import com.datalab.siesta.queryprocessor.storage.DatabaseRepository;
import javassist.bytecode.CodeIterator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public abstract class SparkDatabaseRepository implements DatabaseRepository {

    /**
     * return all the IndexPairs grouped by the eventA and eventB
     *
     * @param pairs
     * @param logname
     * @return
     */
    protected JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>> getAllEventPairs(Set<EventPair> pairs, String logname, Metadata metadata) {
        return null;
    }

    protected JavaPairRDD<Tuple2<String, String>, IndexPair>
    addTimeConstraintFilter(JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>> pairs, List<TimeConstraint> constraints) {
        return null;
    }

    protected JavaPairRDD<Tuple2<String, String>, IndexPair>
    addGapConstraintFilter(JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>> pairs, List<GapConstraint> constraints) {
        return null;
    }

    protected JavaRDD<IndexPair> getPairs(JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>> pairs) {
        return null;
    }

    protected List<Long> getCommonIds(JavaRDD<IndexPair> pairs) {
        return null;
    }

    protected JavaRDD<IndexPair> addFilterIds(JavaRDD<IndexPair> pairs, List<Long> traceIds) {
        return null;
    }

    protected Tuple2<List<TimeConstraintWE>, List<GapConstraintWE>> splitConstraints(Set<EventPair> pairs) {
        List<TimeConstraintWE> tcs = new ArrayList<>();
        List<GapConstraintWE> gcs = new ArrayList<>();
        for(EventPair p : pairs){
            if(p.getConstraint()!=null){
                if(p.getConstraint() instanceof TimeConstraint) tcs.add(new TimeConstraintWE(p));
                if(p.getConstraint() instanceof GapConstraint) gcs.add(new GapConstraintWE(p));
            }
        }
        return new Tuple2<>(tcs,gcs);
    }


}
