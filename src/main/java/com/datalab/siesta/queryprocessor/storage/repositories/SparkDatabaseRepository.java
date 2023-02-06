package com.datalab.siesta.queryprocessor.storage.repositories;

import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraint;
import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraintWE;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraint;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraintWE;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexPair;
import com.datalab.siesta.queryprocessor.model.EventPair;
import com.datalab.siesta.queryprocessor.model.Metadata;
import com.datalab.siesta.queryprocessor.storage.DatabaseRepository;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public abstract class SparkDatabaseRepository implements DatabaseRepository {

    protected SparkSession sparkSession;

    protected JavaSparkContext javaSparkContext;


    @Autowired
    public SparkDatabaseRepository(SparkSession sparkSession, JavaSparkContext javaSparkContext) {
        this.sparkSession = sparkSession;
        this.javaSparkContext = javaSparkContext;
    }

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

    protected JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>>
    addTimeConstraintFilter(JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>> pairs, List<TimeConstraintWE> tc) {
        if(tc.isEmpty()) return pairs;
        Broadcast<List<TimeConstraintWE>> bc = javaSparkContext.broadcast(tc);
        return pairs.mapValues((Function<Iterable<IndexPair>, Iterable<IndexPair>>) groups -> {
            List<IndexPair> indexPairs = (List<IndexPair>) groups;
            ArrayList<IndexPair> response = new ArrayList<>();
            IndexPair f = indexPairs.get(0);
            boolean found = false;
            for (TimeConstraintWE c : bc.getValue()) {
                if (c.isForThisConstraint(f.getEventA(), f.getEventB())) {
                    found = true;
                    for (IndexPair i : indexPairs) {
                        if (!c.isConstraintTrue(i)) response.add(i);
                    }
                    break;
                }
            }
            if (found) {
                return (Iterable<IndexPair>) response;
            } else {
                return groups;
            }
        });
    }

    protected JavaPairRDD<Tuple2<String, String>, Iterable<IndexPair>>
    addGapConstraintFilter(JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>> pairs, List<GapConstraintWE> gc) {
        if(gc.isEmpty()) return pairs;
        Broadcast<List<GapConstraintWE>> bc = javaSparkContext.broadcast(gc);
        return pairs.mapValues((Function<Iterable<IndexPair>, Iterable<IndexPair>>) groups -> {
            List<IndexPair> indexPairs = (List<IndexPair>) groups;
            ArrayList<IndexPair> response = new ArrayList<>();
            IndexPair f = indexPairs.get(0);
            boolean found = false;
            for (GapConstraintWE c : bc.getValue()) {
                if (c.isForThisConstraint(f.getEventA(), f.getEventB())) {
                    found = true;
                    for (IndexPair i : indexPairs) {
                        if (!c.isConstraintTrue(i)) response.add(i);
                    }
                    break;
                }
            }
            if (found) {
                return (Iterable<IndexPair>) response;
            } else {
                return groups;
            }
        });
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
        for (EventPair p : pairs) {
            if (p.getConstraint() != null) {
                if (p.getConstraint() instanceof TimeConstraint) tcs.add(new TimeConstraintWE(p));
                if (p.getConstraint() instanceof GapConstraint) gcs.add(new GapConstraintWE(p));
            }
        }
        return new Tuple2<>(tcs, gcs);
    }


}
