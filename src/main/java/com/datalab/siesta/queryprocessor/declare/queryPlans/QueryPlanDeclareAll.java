package com.datalab.siesta.queryprocessor.declare.queryPlans;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.model.EventPairToNumberOfTrace;
import com.datalab.siesta.queryprocessor.declare.model.OccurrencesPerTrace;
import com.datalab.siesta.queryprocessor.declare.model.UniqueTracesPerEventPair;
import com.datalab.siesta.queryprocessor.declare.model.UniqueTracesPerEventType;
import com.datalab.siesta.queryprocessor.declare.queryPlans.existence.QueryPlanExistences;
import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderedRelations;
import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderedRelationsAlternate;
import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderedRelationsChain;
import com.datalab.siesta.queryprocessor.declare.queryPlans.position.QueryPlanBoth;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseAll;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseExistence;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseOrderedRelations;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponsePosition;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

import java.util.*;
import java.util.stream.Collectors;

@Component
@RequestScope
public class QueryPlanDeclareAll {

    private QueryPlanOrderedRelations queryPlanOrderedRelations;
    private QueryPlanOrderedRelationsChain queryPlanOrderedRelationsChain;
    private QueryPlanOrderedRelationsAlternate queryPlanOrderedRelationsAlternate;
    private QueryPlanExistences queryPlanExistences;
    private QueryPlanBoth queryPlanBoth;
    private JavaSparkContext javaSparkContext;
    private DeclareDBConnector declareDBConnector;
    private Metadata metadata;


    @Autowired
    public QueryPlanDeclareAll(QueryPlanOrderedRelations queryPlanOrderedRelations, QueryPlanOrderedRelationsChain
            queryPlanOrderedRelationsChain, QueryPlanOrderedRelationsAlternate queryPlanOrderedRelationsAlternate,
                               QueryPlanExistences queryPlanExistences, QueryPlanBoth queryPlanBoth,
                               JavaSparkContext javaSparkContext, DeclareDBConnector declareDBConnector) {
        this.queryPlanOrderedRelations = queryPlanOrderedRelations;
        this.queryPlanOrderedRelationsAlternate = queryPlanOrderedRelationsAlternate;
        this.queryPlanOrderedRelationsChain = queryPlanOrderedRelationsChain;
        this.queryPlanExistences = queryPlanExistences;
        this.queryPlanBoth = queryPlanBoth;
        this.javaSparkContext = javaSparkContext;
        this.declareDBConnector = declareDBConnector;
    }

    public QueryResponseAll execute(String logname, double support) {
        //run positions
        this.queryPlanBoth.setMetadata(metadata);
        QueryResponsePosition queryResponsePosition = (QueryResponsePosition) this.queryPlanBoth.execute(logname, support);
        //run existences
        this.queryPlanExistences.setMetadata(metadata);
        this.queryPlanExistences.initResponse();
        Broadcast<Double> bSupport = javaSparkContext.broadcast(support);
        Broadcast<Long> bTotalTraces = javaSparkContext.broadcast(metadata.getTraces());

        //if existence, absence or exactly in modes
        JavaRDD<UniqueTracesPerEventType> uEventType = declareDBConnector.querySingleTableDeclare(logname);
        uEventType.persist(StorageLevel.MEMORY_AND_DISK());
        Map<String, HashMap<Integer, Long>> groupTimes = this.queryPlanExistences.createMapForSingle(uEventType);
        Map<String, Long> singleUnique = this.queryPlanExistences.extractUniqueTracesSingle(groupTimes);
        Broadcast<Map<String, Long>> bUniqueSingle = javaSparkContext.broadcast(singleUnique);

        JavaRDD<UniqueTracesPerEventPair> uPairs = declareDBConnector.queryIndexTableDeclare(logname);
        JavaRDD<EventPairToNumberOfTrace> joined = this.queryPlanExistences.joinUnionTraces(uPairs);
        QueryResponseExistence queryResponseExistence = this.queryPlanExistences.runAll(groupTimes,
                support, joined, bSupport, bTotalTraces, bUniqueSingle, uEventType);
        uEventType.unpersist();

        //create joined table for order relations
        //load data from query table
        JavaRDD<Tuple3<String, String, Long>> indexRDD = declareDBConnector.queryIndexOriginalDeclare(logname)
                .filter(x->!x._1().equals(x._2()));

        JavaPairRDD<Tuple2<String, Long>, List<Integer>> singleRDD = declareDBConnector.querySingleTableAllDeclare(logname);
        singleRDD.persist(StorageLevel.MEMORY_AND_DISK());
        //join based on the first event
        JavaRDD<Tuple5<String, String, Long, List<Integer>, List<Integer>>> joinedOrder = indexRDD.keyBy(r -> new Tuple2<>(r._1(), r._3()))
                .join(singleRDD)
                .map(x -> x._2)
                //join based on the second event
                .keyBy(x -> new Tuple2<>(x._1._2(), x._1._3()))
                .join(singleRDD)
                .map(x -> {
                    String eventA = x._2._1._1._1();//event a
                    String eventB = x._2._1._1._2();//event b
                    List<Integer> f = x._2._1._2; // occurrences of the first event
                    List<Integer> s = x._2._2;//occurrences of the second event
                    long tid = x._1._2; //trace id
                    return new Tuple5<>(eventA, eventB, tid, f, s);
                });
        joinedOrder.persist(StorageLevel.MEMORY_AND_DISK());
        singleRDD.unpersist();
        // extract simple ordered
        JavaRDD<Tuple4<String, String, String, Integer>> cSimple = joinedOrder
                .flatMap((FlatMapFunction<Tuple5<String, String, Long, List<Integer>, List<Integer>>, Tuple4<String, String, String, Integer>>) trace -> {
                    List<Tuple4<String, String, String, Integer>> answer = new ArrayList<>();
                    int s = 0;
                    for (int a : trace._4()) {
                        if (trace._5().stream().anyMatch(y -> y > a)) s += 1;
                    }
                    answer.add(new Tuple4<>(trace._1(), trace._2(), "r", s));
                    s = 0;
                    for (int a : trace._5()) {
                        if (trace._4().stream().anyMatch(y -> y < a)) s += 1;
                    }
                    answer.add(new Tuple4<>(trace._1(), trace._2(), "p", s));
                    return answer.iterator();
                })
                .keyBy(r -> new Tuple3<String, String, String>(r._1(), r._2(), r._3()))
                .reduceByKey((x, y) -> new Tuple4<>(x._1(), x._2(), x._3(), x._4() + y._4()))
                .map(x->x._2);

        cSimple.persist(StorageLevel.MEMORY_AND_DISK());
        Map<String, Long> uEventType2 = declareDBConnector.querySingleTableDeclare(logname)
                .map(x -> {
                    long all = x.getOccurrences().stream().mapToLong(OccurrencesPerTrace::getOccurrences).sum();
                    return new Tuple2<>(x.getEventType(), all);
                }).keyBy(x -> x._1).mapValues(x -> x._2).collectAsMap();
        Broadcast<Map<String, Long>> bUEventTypes = javaSparkContext.broadcast(uEventType2);

        this.queryPlanOrderedRelations.initQueryResponse();
        this.queryPlanOrderedRelations.setMetadata(metadata);
        this.queryPlanOrderedRelations.extendNotSuccession(uEventType2,logname,cSimple);
        this.queryPlanOrderedRelations.filterBasedOnSupport(cSimple, bUEventTypes, support);
        QueryResponseOrderedRelations qSimple = this.queryPlanOrderedRelations.getQueryResponseOrderedRelations();
        cSimple.unpersist();

        // extract alternate ordered

        //alternate filter based on the r and p found on the previous step
        JavaRDD<Tuple4<String, String, String, Integer>> alternate = joinedOrder
                .flatMap((FlatMapFunction<Tuple5<String, String, Long, List<Integer>, List<Integer>>, Tuple4<String, String, String, Integer>>) trace -> {
                    List<Tuple4<String, String, String, Integer>> answer = new ArrayList<>();
                    int s = 0;
                    List<Integer> aList = trace._4().stream().sorted().collect(Collectors.toList());
                    for (int i = 0; i < aList.size() - 1; i++) {
                        int finalI = i;
                        if (trace._5().stream().anyMatch(y -> y > aList.get(finalI) && y < aList.get(finalI + 1)))
                            s += 1;
                    }
                    if (trace._5().stream().anyMatch(y -> y > aList.get(aList.size() - 1))) s += 1;
                    answer.add(new Tuple4<>(trace._1(), trace._2(), "r", s));
                    s = 0;
                    List<Integer> bList = trace._5().stream().sorted().collect(Collectors.toList());
                    for (int i = 1; i < bList.size(); i++) {
                        int finalI = i;
                        if (trace._4().stream().anyMatch(y -> y < bList.get(finalI) && y > bList.get(finalI - 1)))
                            s += 1;
                    }
                    if (trace._4().stream().anyMatch(y -> y < bList.get(0))) s += 1;
                    answer.add(new Tuple4<>(trace._1(), trace._2(), "p", s));
                    return answer.iterator();
                })
                .keyBy(r -> new Tuple3<String, String, String>(r._1(), r._2(), r._3()))
                .reduceByKey((x, y) -> new Tuple4<>(x._1(), x._2(), x._3(), x._4() + y._4()))
                .map(x->x._2);
        this.queryPlanOrderedRelationsAlternate.initQueryResponse();
        this.queryPlanOrderedRelationsAlternate.setMetadata(metadata);
        this.queryPlanOrderedRelationsAlternate.filterBasedOnSupport(alternate, bUEventTypes, support);
        QueryResponseOrderedRelations qAlternate = this.queryPlanOrderedRelationsAlternate.getQueryResponseOrderedRelations();

        //extract chain responses
        JavaRDD<Tuple4<String, String, String, Integer>> chain = joinedOrder
                .flatMap((FlatMapFunction<Tuple5<String, String, Long, List<Integer>, List<Integer>>, Tuple4<String, String, String, Integer>>) trace -> {
                    List<Tuple4<String, String, String, Integer>> answer = new ArrayList<>();
                    int s = 0;
                    for (int a : trace._4()) {
                        if (trace._5().stream().anyMatch(y -> y == a + 1)) s += 1;
                    }
                    answer.add(new Tuple4<>(trace._1(), trace._2(), "r", s));
                    s = 0;
                    for (int a : trace._5()) {
                        if (trace._4().stream().anyMatch(y -> y == a - 1)) s += 1;
                    }
                    answer.add(new Tuple4<>(trace._1(), trace._2(), "p", s));
                    return answer.iterator();
                })
                .keyBy(r -> new Tuple3<String, String, String>(r._1(), r._2(), r._3()))
                .reduceByKey((x, y) -> new Tuple4<>(x._1(), x._2(), x._3(), x._4() + y._4()))
                .map(x->x._2);
        chain.persist(StorageLevel.MEMORY_AND_DISK());
        this.queryPlanOrderedRelationsChain.initQueryResponse();
        this.queryPlanOrderedRelationsChain.setMetadata(metadata);
        this.queryPlanOrderedRelationsChain.extendNotSuccession(uEventType2,logname,cSimple);
        this.queryPlanOrderedRelationsChain.filterBasedOnSupport(chain,bUEventTypes,support);
        QueryResponseOrderedRelations qChain = this.queryPlanOrderedRelationsChain
                .getQueryResponseOrderedRelations();
        chain.unpersist();
        joinedOrder.unpersist();

        //combine all responses
        QueryResponseAll queryResponseAll = new QueryResponseAll(queryResponseExistence, queryResponsePosition, qSimple,
                qAlternate, qChain);
        return queryResponseAll;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }
}
