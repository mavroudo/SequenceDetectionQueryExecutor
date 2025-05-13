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

/**
 * This in a class the contains common logic for all databases that utilize spark (like Cassandra and S3)
 * and therefore it is implemented by both of them
 */
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
     * needs to be implemented by each different connector
     * @param pairs set of the pairs
     * @param logname the log database
     * @return extract the pairs
     */
    protected JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>> getAllEventPairs(Set<EventPair> pairs, String logname, Metadata metadata, Timestamp from, Timestamp till) {
        return null;
    }

    /**
     * return all the IndexPairs grouped by the eventA and eventB
     * needs to be implemented by each different connector
     * @param pairs set of the pairs
     * @param logname the log database
     * @return extract the pairs
     */
    protected JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>> getAllEventPairs(Set<EventPair> pairs, String logname) {
        return null;
    }

    /**
     * Retrieves the appropriate events from the SequenceTable, which contains the original traces
     * @param logname the log database
     * @param traceIds the ids of the traces that will be retrieved
     * @return a map where the key is the trace id and the value is a list of the retrieved events (with their
     *      * timestamps)
     */
    @Override
    public Map<String, List<EventBoth>> querySeqTable(String logname, List<String> traceIds) {
        Broadcast<Set<String>> bTraceIds = javaSparkContext.broadcast(new HashSet<>(traceIds));
        return this.querySequenceTablePrivate(logname, bTraceIds)
                .keyBy((Function<Trace, String>) Trace::getTraceID)
                .mapValues((Function<Trace, List<EventBoth>>) Trace::getEvents)
                .collectAsMap();
    }

    /**
     * Retrieves the appropriate events from the SequenceTable, which contains the original traces
     * @param logname the log database
     * @param traceIds the ids of the traces that will be retrieved
     * @param eventTypes the events that will be retrieved
     * @param from the starting timestamp, set to null if not used
     * @param till the ending timestamp, set to null if not used
     * @return a map where the key is the trace id and the value is a list of the retrieved events (with their
     * timestamps)
     */
    @Override
    public Map<String, List<EventBoth>> querySeqTable(String logname, List<String> traceIds, Set<String> eventTypes, Timestamp from, Timestamp till) {
        Broadcast<Set<String>> bTraceIds = javaSparkContext.broadcast(new HashSet<>(traceIds));
        Broadcast<Set<String>> bevents = javaSparkContext.broadcast(new HashSet<>(eventTypes));
        Broadcast<Timestamp> bFrom = javaSparkContext.broadcast(from);
        Broadcast<Timestamp> bTill = javaSparkContext.broadcast(till);
        JavaRDD<Trace> df = this.querySequenceTablePrivate(logname, bTraceIds)
                .map((Function<Trace, Trace>) trace -> {
                    trace.filter(bFrom.getValue(), bTill.getValue());
                    return trace;
                });
        return df.keyBy((Function<Trace, String>) Trace::getTraceID)
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
    protected JavaRDD<Trace> querySequenceTablePrivate(String logname, Broadcast<Set<String>> bTraceIds) {
        return null;
    }

    /**
     * Retrieves data from the primary inverted index
     * @param pairs a set of the pairs that we need to retrieve information for
     * @param logname the log database
     * @return the corresponding records from the index
     */
    @Override
    public IndexRecords queryIndexTable(Set<EventPair> pairs, String logname) {
        List<Tuple2<Tuple2<String, String>, Iterable<IndexPair>>> results = this.getAllEventPairs(pairs, logname)
                .collect();
        return new IndexRecords(results);
    }

    /**
     * Retrieves data from the primary inverted index
     * @param pairs a set of the pairs that we need to retrieve information for
     * @param logname the log database
     * @param metadata the metadata for this log database
     * @param from the starting timestamp, set to null if not used
     * @param till the ending timestamp, set to null if not used
     * @return the corresponding records from the index
     */
    @Override
    public IndexRecords queryIndexTable(Set<EventPair> pairs, String logname, Metadata metadata, Timestamp from, Timestamp till) {
        List<Tuple2<Tuple2<String, String>, Iterable<IndexPair>>> results = this.getAllEventPairs(pairs, logname, metadata, from, till)
                .collect();
        return new IndexRecords(results);
    }

    protected JavaRDD<IndexPair> getPairs(JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>> pairs) {
        return pairs.flatMap((FlatMapFunction<Tuple2<Tuple2<String, String>, Iterable<IndexPair>>, IndexPair>) g -> g._2.iterator());
    }

    /**
     * Extract the ids of the traces that contains all the provided pairs
     * @param pairs pairs retrieved from the storage
     * @param trueEventPairs the pairs that required to appear in a trace in order to be a candidate
     * @return the candidate trace ids
     */
    protected List<String> getCommonIds(JavaRDD<IndexPair> pairs, Set<EventPair> trueEventPairs) {
        Broadcast<Set<EventPair>> truePairs = javaSparkContext.broadcast(trueEventPairs);
        return pairs.map((Function<IndexPair, Tuple3<String, String, String>>) pair ->
                        new Tuple3<>(pair.getEventA(), pair.getEventB(), pair.getTraceId()))
                .distinct() //remove all the duplicate event pairs that refer to the same trace
                .groupBy((Function<Tuple3<String, String, String>, String>) Tuple3::_3)
                .map((Function<Tuple2<String, Iterable<Tuple3<String, String, String>>>, Tuple2<String,Integer>>)x->{
                    AtomicInteger acc = new AtomicInteger(0);
                    x._2.forEach(pair-> {
                        Optional<EventPair> op =truePairs.getValue().stream().filter(y->y.getEventA().getName().equals(pair._1())&&
                                y.getEventB().getName().equals(pair._2())).findFirst();
                        if(op.isPresent()) acc.incrementAndGet();
                    });
                    return new Tuple2<>(x._1, acc.get());
                } )
                .filter((Function<Tuple2<String, Integer>, Boolean>) p -> p._2 == truePairs.getValue().size())
                .map((Function<Tuple2<String, Integer>, String>) p -> p._1)
                .collect();
    }

    /**
     * Filters te results based on the starting and ending timestamp
     * @param pairs the retrieved pairs from the IndexTable
     * @param traceIds the trace ids that contains all the required pairs
     * @param from the starting timestamp, set to null if not used
     * @param till the ending timestamp, set to null if not used
     * @return the intermediate results, i.e. the candidate traces before remove false positives
     */
    protected IndexMiddleResult addFilterIds(JavaRDD<IndexPair> pairs, List<String> traceIds, Timestamp from, Timestamp till) {
        Broadcast<Set<String>> bTraces = javaSparkContext.broadcast(new HashSet<>(traceIds));
        Broadcast<Timestamp> bFrom = javaSparkContext.broadcast(from);
        Broadcast<Timestamp> bTill = javaSparkContext.broadcast(till);
        JavaRDD<IndexPair> filtered = pairs.filter((Function<IndexPair, Boolean>) pair ->
                bTraces.getValue().contains(pair.getTraceId()));
        IndexMiddleResult imr = new IndexMiddleResult();
        imr.setTrace_ids(traceIds);
        Map<String, List<Event>> events = filtered.flatMap((FlatMapFunction<IndexPair, Event>) indexPair -> indexPair.getEvents().iterator())
                .groupBy((Function<Event, String>) Event::getTraceID)
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


    /**
     * Detects the traces that contain all the given event pairs
     * @param logname the log database
     * @param combined a list where each event pair is combined with the according stats from the CountTable
     * @param metadata the log database metadata
     * @param expairs the event pairs extracted from the query
     * @param from the starting timestamp, set to null if not used
     * @param till the ending timestamp, set to null if not used
     * @return the traces that contain all the pairs. It will be then processed by SASE in order to remove false
     * positives.
     */
    @Override
    public IndexMiddleResult patterDetectionTraceIds(String logname, List<Tuple2<EventPair, Count>> combined, Metadata metadata,
                                                     ExtractedPairsForPatternDetection expairs, Timestamp from, Timestamp till) {
        Set<EventPair> pairs = combined.stream().map(x -> x._1).collect(Collectors.toSet());
        JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>> gpairs = this.getAllEventPairs(pairs, logname, metadata, from, till);
        JavaRDD<IndexPair> indexPairs = this.getPairs(gpairs);
        indexPairs.persist(StorageLevel.MEMORY_AND_DISK());
        List<String> traces = this.getCommonIds(indexPairs, expairs.getTruePairs());
        IndexMiddleResult imr = this.addFilterIds(indexPairs, traces, from, till);
        indexPairs.unpersist();
        return imr;
    }

    /**
     * Should be overridden by any storage that uses spark
     *
     * @param logname    The name of the Log
     * @param traceIds   The traces we want to detect
     * @param eventTypes The event types to be collected
     * @return a JavaRDD<EventBoth> that will be used in querySingleTable and querySingleTableGroups
     */
    protected JavaRDD<EventBoth> getFromSingle(String logname, Set<String> traceIds, Set<String> eventTypes) {
        Broadcast<Set<String>> bTraces = javaSparkContext.broadcast(traceIds);
        return queryFromSingle(logname,eventTypes).filter((Function<EventBoth, Boolean>) event->
                bTraces.getValue().contains(event.getTraceID()));
    }

    protected JavaRDD<EventBoth> queryFromSingle(String logname, Set<String> eventTypes){
        return null;
    }

    @Override
    public Map<String, List<EventBoth>> querySingleTable(String logname, Set<String> eventTypes) {
        JavaRDD<EventBoth> events = queryFromSingle(logname, eventTypes);
        JavaPairRDD<String, Iterable<EventBoth>> pairs = events.groupBy((Function<EventBoth, String>) Event::getName);
        return pairs.mapValues((Function<Iterable<EventBoth>, List<EventBoth>>) e -> {
            List<EventBoth> tempList = new ArrayList<>();
            for (EventBoth ev : e) {
                tempList.add(ev);
            }
            return tempList;
        }).collectAsMap();
    }

    /**
     * Retrieves the appropriate events from the SingleTable, which contains the single inverted index
     * @param logname the log database
     * @param traceIds the ids of the traces that wil be retrieved
     * @param eventTypes the events that will we retrieved
     * @return a list of all the retrieved events (wth their timestamps)
     */
    @Override
    public List<EventBoth> querySingleTable(String logname, Set<String> traceIds, Set<String> eventTypes) {
        return this.getFromSingle(logname, traceIds, eventTypes).collect();
    }

    /**
     * Retrieves the appropriate events from the SingleTable, which contains the single inverted index
     * @param logname the log database
     * @param groups a list of the groups as defined in the query
     * @param eventTypes the events that will we retrieved
     * @return a map where the key is the group id and the value is a list of the retrieved events (with their t
     * imestamps)
     */
    @Override
    public Map<Integer, List<EventBoth>> querySingleTableGroups(String logname, List<Set<String>> groups, Set<String> eventTypes) {
        Set<String> allTraces = groups.stream()
                .flatMap((java.util.function.Function<Set<String>, Stream<String>>) Collection::stream)
                .collect(Collectors.toSet());
        Broadcast<List<Set<String>>> bgroups = javaSparkContext.broadcast(groups);
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
                //maintain only these groups that contain all the event types in the query
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

    //Below are for Declare//




}
