package com.datalab.siesta.queryprocessor.storage.repositories;

import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraint;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraint;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexPair;
import com.datalab.siesta.queryprocessor.model.EventPair;
import com.datalab.siesta.queryprocessor.storage.DatabaseRepository;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

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
    protected JavaPairRDD<Tuple2<String, String>, IndexPair> getAllEventPairs(Set<EventPair> pairs, String logname) {
        return null;
    }

    protected JavaPairRDD<Tuple2<String, String>, IndexPair>
    addTimeConstraintFilter(JavaPairRDD<Tuple2<String, String>, IndexPair> pairs, List<TimeConstraint> constraints) {
        return null;
    }

    protected JavaPairRDD<Tuple2<String, String>, IndexPair>
    addGapConstraintFilter(JavaPairRDD<Tuple2<String, String>, IndexPair> pairs, List<GapConstraint> constraints) {
        return null;
    }

    protected JavaRDD<IndexPair> getPairs(JavaPairRDD<Tuple2<String, String>, IndexPair> pairs){
        return null;
    }

    protected List<Long> getCommonIds(JavaRDD<IndexPair> pairs){
        return null;
    }

    protected JavaRDD<IndexPair> addFilterIds(JavaRDD<IndexPair> pairs, List<Long> traceIds){
        return null;
    }



}
