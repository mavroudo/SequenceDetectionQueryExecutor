package com.datalab.siesta.queryprocessor.storage.repositories;

import com.datalab.siesta.queryprocessor.model.DBModel.*;
import com.datalab.siesta.queryprocessor.model.Events.*;
import com.datalab.siesta.queryprocessor.model.ExtractedPairsForPatternDetection;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.DatabaseRepository;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import scala.Tuple2;
import scala.Tuple3;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class SparkDatabaseRepository implements DatabaseRepository {

    protected SparkSession sparkSession;

    protected JavaSparkContext javaSparkContext;

    protected Utils utils;

    @Autowired
    public SparkDatabaseRepository(SparkSession sparkSession, JavaSparkContext javaSparkContext, Utils utils) {
        this.sparkSession = sparkSession;
        this.javaSparkContext = javaSparkContext;
        this.utils = utils;
    }

    /**
     * return all the IndexPairs grouped by the eventA and eventB
     *
     * @param pairs
     * @param logname
     * @return
     */
    protected JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>> getAllEventPairs(Set<EventPair> pairs, String logname, Metadata metadata, Timestamp from, Timestamp till) {
        return null;
    }

    @Override
    public Map<Long, List<EventBoth>> querySeqTable(String logname, List<Long> traceIds) {
        Broadcast<Set<Long>> bTraceIds = javaSparkContext.broadcast(new HashSet<>(traceIds));
        return this.querySequenceTablePrivate(logname, bTraceIds)
                .keyBy((Function<Trace, Long>) Trace::getTraceID)
                .mapValues((Function<Trace, List<EventBoth>>) Trace::getEvents)
                .collectAsMap();
    }

    @Override
    public Map<Long, List<EventBoth>> querySeqTable(String logname, List<Long> traceIds, Set<String> eventTypes, Timestamp from, Timestamp till) {
        Broadcast<Set<Long>> bTraceIds = javaSparkContext.broadcast(new HashSet<>(traceIds));
        Broadcast<Set<String>> bevents = javaSparkContext.broadcast(new HashSet<>(eventTypes));
        Broadcast<Timestamp> bFrom = javaSparkContext.broadcast(from);
        Broadcast<Timestamp> bTill = javaSparkContext.broadcast(till);
        JavaRDD<Trace> df = this.querySequenceTablePrivate(logname, bTraceIds)
                .map((Function<Trace, Trace>) trace -> {
                    trace.filter(bFrom.getValue(), bTill.getValue());
                    return trace;
                });
        return df.keyBy((Function<Trace, Long>) Trace::getTraceID)
                .mapValues((Function<Trace, List<EventBoth>>) trace -> trace.clearTrace(bevents.getValue()))
                .collectAsMap();
    }

    /**
     * This function reads data from the Sequence table into a JavaRDD, any database that utilizes spark should
     * override it
     *
     * @param logname   Name of the log
     * @param bTraceIds broadcasted the values of the trace ids we are interested in
     * @return a JavaRDD<Trace>
     */
    protected JavaRDD<Trace> querySequenceTablePrivate(String logname, Broadcast<Set<Long>> bTraceIds) {
        return null;
    }

    @Override
    public IndexRecords queryIndexTable(Set<EventPair> pairs, String logname, Metadata metadata) {
        List<Tuple2<Tuple2<String, String>, Iterable<IndexPair>>> results = this.getAllEventPairs(pairs, logname, metadata, null, null)
                .collect();
        return new IndexRecords(results);
    }

    @Override
    public IndexRecords queryIndexTable(Set<EventPair> pairs, String logname, Metadata metadata, Timestamp from, Timestamp till) {
        List<Tuple2<Tuple2<String, String>, Iterable<IndexPair>>> results = this.getAllEventPairs(pairs, logname, metadata, from, till)
                .collect();
        return new IndexRecords(results);
    }

    protected JavaRDD<IndexPair> getPairs(JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>> pairs) {
        return pairs.flatMap((FlatMapFunction<Tuple2<Tuple2<String, String>, Iterable<IndexPair>>, IndexPair>) g -> g._2.iterator());
    }

    protected List<Long> getCommonIds(JavaRDD<IndexPair> pairs, Set<EventPair> trueEventPairs) {
        Broadcast<Set<EventPair>> truePairs = javaSparkContext.broadcast(trueEventPairs);
        return pairs.map((Function<IndexPair, Tuple3<String, String, Long>>) pair ->
                        new Tuple3<>(pair.getEventA(), pair.getEventB(), pair.getTraceId()))
                .distinct() //remove all the duplicate event pairs that refer to the same trace
                .groupBy((Function<Tuple3<String, String, Long>, Long>) Tuple3::_3)
                .map((Function<Tuple2<Long, Iterable<Tuple3<String, String, Long>>>, Tuple2<Long,Integer>>)x->{
                    AtomicInteger acc = new AtomicInteger(0);
                    x._2.forEach(pair-> {
                        Optional<EventPair> op =truePairs.getValue().stream().filter(y->y.getEventA().getName().equals(pair._1())&&
                                y.getEventB().getName().equals(pair._2())).findFirst();
                        if(op.isPresent()) acc.incrementAndGet();
                    });
                    return new Tuple2<>(x._1, acc.get());
                } )
                .filter((Function<Tuple2<Long, Integer>, Boolean>) p -> p._2 == truePairs.getValue().size())
                .map((Function<Tuple2<Long, Integer>, Long>) p -> p._1)
                .collect();
    }

    protected IndexMiddleResult addFilterIds(JavaRDD<IndexPair> pairs, List<Long> traceIds, Timestamp from, Timestamp till) {
        Broadcast<Set<Long>> bTraces = javaSparkContext.broadcast(new HashSet<>(traceIds));
        Broadcast<Timestamp> bFrom = javaSparkContext.broadcast(from);
        Broadcast<Timestamp> bTill = javaSparkContext.broadcast(till);
        JavaRDD<IndexPair> filtered = pairs.filter((Function<IndexPair, Boolean>) pair ->
                bTraces.getValue().contains(pair.getTraceId()));
        IndexMiddleResult imr = new IndexMiddleResult();
        imr.setTrace_ids(traceIds);
        Map<Long, List<Event>> events = filtered.flatMap((FlatMapFunction<IndexPair, Event>) indexPair -> indexPair.getEvents().iterator())
                .groupBy((Function<Event, Long>) Event::getTraceID)
                .mapValues((Function<Iterable<Event>, List<Event>>) p -> {
                    Set<Event> eventSet = new HashSet<>();
                    for (Event ev : p) {
                        if (ev instanceof EventPos) eventSet.add(ev);
                        else {
                            EventTs et = (EventTs) ev;
                            if (bFrom.value() != null & bTill.value() != null) {
                                if (!et.getTimestamp().before(bFrom.value()) && !et.getTimestamp().after(bTill.value())) {
                                    eventSet.add(ev);
                                }
                            } else if (bFrom.value() != null) {
                                if (!et.getTimestamp().before(bFrom.value())) {
                                    eventSet.add(ev);
                                }
                            } else if (bTill.value() != null) {
                                if (!et.getTimestamp().after(bTill.value())) {
                                    eventSet.add(ev);
                                }
                            } else {
                                eventSet.add(ev);
                            }
                        }
                    }
                    List<Event> eventsList = new ArrayList<>(eventSet);
                    Collections.sort(eventsList);
                    return eventsList;
                })
                .collectAsMap();
        imr.setEvents(events);
        return imr;
    }


    @Override
    public IndexMiddleResult patterDetectionTraceIds(String logname, List<Tuple2<EventPair, Count>> combined, Metadata metadata,
                                                     ExtractedPairsForPatternDetection expairs, Timestamp from, Timestamp till) {
        Set<EventPair> pairs = combined.stream().map(x -> x._1).collect(Collectors.toSet());
        JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>> gpairs = this.getAllEventPairs(pairs, logname, metadata, from, till);
        JavaRDD<IndexPair> indexPairs = this.getPairs(gpairs);
        indexPairs.persist(StorageLevel.MEMORY_AND_DISK());
        List<Long> traces = this.getCommonIds(indexPairs, expairs.getTruePairs());
        IndexMiddleResult imr = this.addFilterIds(indexPairs, traces, from, till);
        indexPairs.unpersist();
        return imr;
    }

    /**
     * Should be override by any storage that uses spark
     *
     * @param logname    The name of the Log
     * @param traceIds   The traces we want to detect
     * @param eventTypes The event types to be collected
     * @return a JavaRDD<EventBoth> that will be used in querySingleTable and querySingleTableGroups
     */
    protected JavaRDD<EventBoth> getFromSingle(String logname, Set<Long> traceIds, Set<String> eventTypes) {
        return null;
    }

    @Override
    public List<EventBoth> querySingleTable(String logname, Set<Long> traceIds, Set<String> eventTypes) {
        return this.getFromSingle(logname, traceIds, eventTypes).collect();
    }

    @Override
    public Map<Integer, List<EventBoth>> querySingleTableGroups(String logname, List<Set<Long>> groups, Set<String> eventTypes) {
        Set<Long> allTraces = groups.stream()
                .flatMap((java.util.function.Function<Set<Long>, Stream<Long>>) Collection::stream)
                .collect(Collectors.toSet());
        Broadcast<List<Set<Long>>> bgroups = javaSparkContext.broadcast(groups);
        Broadcast<Integer> bEventTypesSize = javaSparkContext.broadcast(eventTypes.size());
        JavaRDD<EventBoth> eventRDD = this.getFromSingle(logname, allTraces, eventTypes);
        Map<Integer, List<EventBoth>> response = eventRDD.map((Function<EventBoth, Tuple2<Integer, EventBoth>>) event -> {
                    for (int g = 0; g < bgroups.value().size(); g++) {
                        if (bgroups.value().get(g).contains(event.getTraceID())) return new Tuple2<>(g + 1, event);
                    }
                    return new Tuple2<>(-1, event);
                })
                .filter((Function<Tuple2<Integer, EventBoth>, Boolean>) event -> event._1 != -1)
                .groupBy((Function<Tuple2<Integer, EventBoth>, Integer>) event -> event._1)
                //maintain only these groups that contain all of the event types in the query
                .filter((Function<Tuple2<Integer, Iterable<Tuple2<Integer, EventBoth>>>, Boolean>) group -> {
                    Set<String> events = new HashSet<>();
                    group._2.forEach(x -> events.add(x._2.getName()));
                    return events.size() == bEventTypesSize.value();
                })
                .mapValues((Function<Iterable<Tuple2<Integer, EventBoth>>, List<EventBoth>>) group -> {
                    List<EventBoth> eventBoth = new ArrayList<>();
                    for (Tuple2<Integer, EventBoth> e : group) {
                        eventBoth.add(e._2);
                    }
                    return eventBoth.stream().sorted().collect(Collectors.toList());
                }).collectAsMap();
        return response;

    }


}
