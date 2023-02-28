package com.datalab.siesta.queryprocessor.storage.repositories;

import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraintWE;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraintWE;
import com.datalab.siesta.queryprocessor.model.DBModel.*;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.Event;
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
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.mapping.Tuple;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.util.*;
import java.util.stream.Collectors;

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

    @Override
    public Map<Long, List<EventBoth>> querySeqTable(String logname, List<Long> traceIds) {
        Broadcast<Set<Long>> bTraceIds= javaSparkContext.broadcast(new HashSet<>(traceIds));
        return this.querySequenceTablePrivate(logname,bTraceIds)
                .keyBy((Function<Trace, Long>) Trace::getTraceID)
                .mapValues((Function<Trace, List<EventBoth>>) Trace::getEvents)
                .collectAsMap();
    }

    @Override
    public Map<Long, List<EventBoth>> querySeqTable(String logname, List<Long> traceIds, List<String> eventTypes) {
        Broadcast<Set<Long>> bTraceIds= javaSparkContext.broadcast(new HashSet<>(traceIds));
        Broadcast<Set<String>> bevents = javaSparkContext.broadcast(new HashSet<>(eventTypes));
        JavaRDD<Trace> df = this.querySequenceTablePrivate(logname,bTraceIds);
        return df.keyBy((Function<Trace, Long>) Trace::getTraceID)
                .mapValues((Function<Trace, List<EventBoth>>) trace -> trace.clearTrace(bevents.getValue()))
                .collectAsMap();
    }

    /**
     * This function reads data from the Sequence table into a JavaRDD, any database that utilizes spark should
     * override it
     * @param logname Name of the log
     * @param bTraceIds broadcasted the values of the trace ids we are interested in
     * @return a JavaRDD<Trace>
     */
    protected JavaRDD<Trace> querySequenceTablePrivate(String logname, Broadcast<Set<Long>> bTraceIds){
        return null;
    }

    @Override
    public IndexRecords queryIndexTable(Set<EventPair> pairs, String logname, Metadata metadata) {
        List<Tuple2<Tuple2<String, String>, Iterable<IndexPair>>> results = this.getAllEventPairs(pairs,logname,metadata)
                .collect();
        return new IndexRecords(results);

    }

    protected JavaRDD<IndexPair> getPairs(JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>> pairs) {
        return  pairs.flatMap((FlatMapFunction<Tuple2<Tuple2<String, String>, Iterable<IndexPair>>, IndexPair>) g-> g._2.iterator());
    }

    protected List<Long> getCommonIds(JavaRDD<IndexPair> pairs, int minPairs) {
        Broadcast<Integer> bminPairs = javaSparkContext.broadcast(minPairs);
        return pairs.map((Function<IndexPair, Tuple3<String,String,Long>>) pair->
                new Tuple3<>(pair.getEventA(), pair.getEventB(),pair.getTraceId()))
                .distinct() //remove all the duplicate event pairs that refer to the same trace
                .map((Function<Tuple3<String, String, Long>, Tuple2<Long,Long>>) p-> new Tuple2<>(p._3(),1L) )
                .keyBy((Function<Tuple2<Long, Long>, Long>) p-> p._1 )
                .reduceByKey((Function2<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>) (p1,p2)->
                    new Tuple2<>(p1._1,p1._2+p2._2))
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

    @Override
    public IndexMiddleResult patterDetectionTraceIds(String logname, List<Tuple2<EventPair, Count>> combined, Metadata metadata, int minPairs) {
        Set<EventPair> pairs = combined.stream().map(x -> x._1).collect(Collectors.toSet());
        JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>> gpairs =this.getAllEventPairs(pairs, logname, metadata);
        JavaRDD<IndexPair> indexPairs = this.getPairs(gpairs);
        indexPairs.persist(StorageLevel.MEMORY_AND_DISK());
        List<Long> traces = this.getCommonIds(indexPairs,minPairs);
        IndexMiddleResult imr = this.addFilterIds(indexPairs,traces);
        indexPairs.unpersist();
        return imr;
    }




}
