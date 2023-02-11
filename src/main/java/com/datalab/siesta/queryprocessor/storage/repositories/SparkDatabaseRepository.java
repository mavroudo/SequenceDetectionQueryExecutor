package com.datalab.siesta.queryprocessor.storage.repositories;

import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraintWE;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraintWE;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexMiddleResult;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.storage.DatabaseRepository;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import scala.Tuple2;

import java.util.*;

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

    protected void
    addTimeConstraintFilter(JavaPairRDD<Tuple2<String, String>, Iterable<IndexPair>> pairs, List<TimeConstraintWE> tc) {
        if(tc.isEmpty()) return;
        Broadcast<List<TimeConstraintWE>> bc = javaSparkContext.broadcast(tc);
        pairs.mapValues((Function<Iterable<IndexPair>, Iterable<IndexPair>>) groups -> {
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

    protected void
    addGapConstraintFilter(JavaPairRDD<Tuple2<String, String>, Iterable<IndexPair>> pairs, List<GapConstraintWE> gc) {
        if(gc.isEmpty()) return;
        Broadcast<List<GapConstraintWE>> bc = javaSparkContext.broadcast(gc);
        pairs.mapValues((Function<Iterable<IndexPair>, Iterable<IndexPair>>) groups -> {
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
        return  pairs.flatMap((FlatMapFunction<Tuple2<Tuple2<String, String>, Iterable<IndexPair>>, IndexPair>) g-> g._2.iterator());
    }

    protected List<Long> getCommonIds(JavaRDD<IndexPair> pairs, int minPairs) {
        Broadcast<Integer> bminPairs = javaSparkContext.broadcast(minPairs);
        return pairs.map((Function<IndexPair, Tuple2<Long, Long>>) p-> new Tuple2<>(p.getTraceId(),1L))
                .keyBy((Function<Tuple2<Long, Long>, Long>) p-> p._1 )
                .reduceByKey((Function2<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>) (p1,p2)->
                    new Tuple2<>(p1._1,p1._2+p2._2)
                )
                .filter((Function<Tuple2<Long, Tuple2<Long, Long>>, Boolean>) p-> p._2._2>= bminPairs.getValue())
                .map((Function<Tuple2<Long, Tuple2<Long, Long>>, Long>)p->p._1 )
                .collect();
    }

    protected IndexMiddleResult addFilterIds(JavaRDD<IndexPair> pairs, List<Long> traceIds) {
        Broadcast<Set<Long>> bTraces = javaSparkContext.broadcast(new HashSet<>(traceIds));
        JavaRDD<IndexPair> filtered = pairs.filter((Function<IndexPair, Boolean>) pair->
                bTraces.getValue().contains(pair.getTraceId()));
        IndexMiddleResult imr = new IndexMiddleResult();
        imr.setTrace_ids(traceIds);
        Map<Long,List<Event>> events = filtered.flatMap((FlatMapFunction<IndexPair, Event>) indexPair-> indexPair.getEvents().iterator())
                .groupBy((Function<Event, Long>) Event::getTraceID)
                .mapValues((Function<Iterable<Event>, List<Event>>) p-> {
                    Set<Event> e = new HashSet<>();
                    for(Event ev : p){
                        e.add(ev);
                    }
                    List<Event> eventsList = new ArrayList<>(e);
                    Collections.sort(eventsList);
                    return eventsList;
                } )
                        .collectAsMap();
        imr.setEvents(events);
        return imr;
    }




}
