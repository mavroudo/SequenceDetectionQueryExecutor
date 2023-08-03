package com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.model.EventPairSupport;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseOrderedRelations;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexPair;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
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
public class QueryPlanOrderedRelations {

    protected Metadata metadata;
    protected DeclareDBConnector declareDBConnector;
    protected JavaSparkContext javaSparkContext;
    protected QueryResponseOrderedRelations queryResponseOrderedRelations;
    protected OrderedRelationsUtilityFunctions utils;

    @Autowired
    public QueryPlanOrderedRelations(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext,
    OrderedRelationsUtilityFunctions utils) {
        this.declareDBConnector = declareDBConnector;
        this.javaSparkContext = javaSparkContext;
        this.utils=utils;
    }

    public void initQueryResponse(){
        this.queryResponseOrderedRelations = new QueryResponseOrderedRelations("simple");
    }


    public QueryResponse execute(String logname, String constraint, double support) {
        //query IndexTable
        JavaRDD<IndexPair> indexPairsRDD = declareDBConnector.queryIndexTableAllDeclare(logname);
        indexPairsRDD.persist(StorageLevel.MEMORY_AND_DISK());
        //join tables using joinTables and flat map to get the single events
        JavaRDD<Tuple5<String, String, Long, Set<Integer>, Set<Integer>>> joined = joinTables(indexPairsRDD);
        //count the occurrences using the evaluate constraints
        JavaRDD<Tuple4<String, String, String, Integer>> c = evaluateConstraint(joined, constraint);
        //filter based on the values of the SingleTable and the provided support and write to the response
        Map<String, Long> uEventType = declareDBConnector.querySingleTableDeclare(logname)
                .map(x -> {
                    long all = x.getOccurrences().stream().mapToLong(y -> y._2).sum();
                    return new Tuple2<>(x.getEventType(), all);
                }).keyBy(x -> x._1).mapValues(x -> x._2).collectAsMap();
        Broadcast<Map<String, Long>> bUEventTypes = javaSparkContext.broadcast(uEventType);
        filterBasedOnSupport(c, bUEventTypes, support);
        indexPairsRDD.unpersist();
        return this.queryResponseOrderedRelations;
    }

    public JavaRDD<Tuple5<String, String, Long, Set<Integer>, Set<Integer>>> joinTables(JavaRDD<IndexPair> indexPairsRDD) {

        JavaPairRDD<String, Iterable<IndexPair>> singleGrouped = indexPairsRDD
                .filter(x -> x.getEventA().equals(x.getEventB()))
                .groupBy(IndexPair::getEventA);

        JavaPairRDD<Tuple2<String, String>, Tuple3<String, String, List<IndexPair>>> singles = singleGrouped
                .cartesian(singleGrouped)
                .filter(x -> !x._1._1.equals(x._2._1))
                .map(x -> {
                    List<IndexPair> ips = new ArrayList<>();
                    x._1._2.forEach(ips::add);
                    x._2._2.forEach(ips::add);
                    return new Tuple3<>(x._1._1, x._2._1, ips);
                })
                .keyBy(x -> new Tuple2<>(x._1(), x._2()));

        JavaPairRDD<Tuple2<String, String>, Iterable<IndexPair>> grouped = indexPairsRDD
                .filter(x -> !x.getEventA().equals(x.getEventB()))
                .groupBy(x -> new Tuple2<>(x.getEventA(), x.getEventB()));


        JavaRDD<Tuple5<String, String, Long, Set<Integer>, Set<Integer>>> g = grouped.leftOuterJoin(singles)
                .flatMap(x -> { //put each index pair (with different type of events) in the map structure
                    Map<Long, Map<Tuple2<String, String>, Tuple2<Set<Integer>, Set<Integer>>>> totalAbomination =
                            new HashMap<>();
                    x._2._1.forEach(ip -> {
                        Tuple2<String, String> etPair = new Tuple2<>(ip.getEventA(), ip.getEventB());
                        if (!totalAbomination.containsKey(ip.getTraceId())) {
                            Map<Tuple2<String, String>, Tuple2<Set<Integer>, Set<Integer>>> in = new HashMap<>();
                            Set<Integer> s1 = new HashSet<>();
                            s1.add(ip.getPositionA());
                            Set<Integer> s2 = new HashSet<>();
                            s2.add(ip.getPositionB());
                            in.put(etPair, new Tuple2<>(s1, s2));
                            totalAbomination.put(ip.getTraceId(), in);
                        } else {
                            if (!totalAbomination.get(ip.getTraceId()).containsKey(etPair)) {
                                Set<Integer> s1 = new HashSet<>();
                                s1.add(ip.getPositionA());
                                Set<Integer> s2 = new HashSet<>();
                                s2.add(ip.getPositionB());
                                totalAbomination.get(ip.getTraceId()).put(etPair, new Tuple2<>(s1, s2));
                            } else {
                                totalAbomination.get(ip.getTraceId()).get(etPair)._1.add(ip.getPositionA());
                                totalAbomination.get(ip.getTraceId()).get(etPair)._2.add(ip.getPositionB());
                            }
                        }
                    });
                    Iterable<IndexPair> s = x._2._2.orElse(new Tuple3<>("", "", new ArrayList<>()))._3();
                    s.forEach(ip -> {
                        if (totalAbomination.containsKey(ip.getTraceId())) {
                            Set<Tuple2<String, String>> etPairs = totalAbomination.get(ip.getTraceId()).keySet();
                            for (Tuple2<String, String> etPair : etPairs) {
                                if (etPair._1.equals(ip.getEventA())) { // add to the first list
                                    totalAbomination.get(ip.getTraceId()).get(etPair)._1.add(ip.getPositionA());
                                    totalAbomination.get(ip.getTraceId()).get(etPair)._1.add(ip.getPositionB());
                                } else if (etPair._2.equals(ip.getEventA())) {
                                    totalAbomination.get(ip.getTraceId()).get(etPair)._2.add(ip.getPositionA());
                                    totalAbomination.get(ip.getTraceId()).get(etPair)._2.add(ip.getPositionB());
                                }

                            }
                        }
                    });
                    return totalAbomination.entrySet().stream().flatMap(id -> {
                        return id.getValue().entrySet().stream().map(inner -> {
                            return new Tuple5<>(inner.getKey()._1, inner.getKey()._2, id.getKey(), inner.getValue()._1,
                                    inner.getValue()._2);
                        });
                    }).collect(Collectors.toList()).iterator();
                });
        return g;

    }

    //(eventA,eventB, constraint (r for response and p for precedence) ) key
    // (eventA,eventB,
    public JavaRDD<Tuple4<String, String, String, Integer>> evaluateConstraint
    (JavaRDD<Tuple5<String, String, Long, Set<Integer>, Set<Integer>>> joined, String constraint) {

        JavaRDD<Tuple4<String, String, String, Integer>> tuple4JavaRDD;
        switch (constraint) {
            case "response":
                tuple4JavaRDD = joined.map(utils::countResponse);
                break;
            case "precedence":
                tuple4JavaRDD = joined.map(utils::countPrecedence);
                break;
            default:
                tuple4JavaRDD =joined.map(utils::countPrecedence).union(joined.map(utils::countResponse));
                break;
        }
        return tuple4JavaRDD
                .keyBy(y -> new Tuple3<>(y._1(), y._2(), y._3()))
                .reduceByKey((x, y) -> new Tuple4<>(x._1(), y._2(), y._3(), x._4() + y._4()))
                .map(x -> x._2);

    }



    //filters based on support and constraint required and write them to the response
    public void filterBasedOnSupport(JavaRDD<Tuple4<String, String, String, Integer>> c,
             Broadcast<Map<String, Long>> bUEventType, double support) {
        Broadcast<Double> bSupport = javaSparkContext.broadcast(support);
        JavaRDD<Tuple2<String, EventPairSupport>> intermediate = c.map(x -> {
            long total;
            if (x._3().equals("r")) { //response
                total = bUEventType.getValue().get(x._1());
            } else {
                total = bUEventType.getValue().get(x._2());
            }
            long found = x._4();
            return new Tuple2<>(x._3(), new EventPairSupport(x._1(), x._2(), (double) found / total));
        });
        intermediate.persist(StorageLevel.MEMORY_AND_DISK());
        List<Tuple2<String, EventPairSupport>> detected = intermediate
                .filter(x -> x._2.getSupport() >= bSupport.getValue())
                .collect();
        List<EventPairSupport> responses = detected.stream().filter(x -> x._1.equals("r"))
                .map(x -> x._2).collect(Collectors.toList());
        List<EventPairSupport> precedence = detected.stream().filter(x -> x._1.equals("p"))
                .map(x -> x._2).collect(Collectors.toList());
        if (!precedence.isEmpty() && !responses.isEmpty()) { //we are looking for succession and no succession
            setResults(responses, "response");
            setResults(precedence, "precedence");
            //handle succession
            List<EventPairSupport> succession = new ArrayList<>();
            for(EventPairSupport r1:responses){
                for(EventPairSupport p1: precedence){
                    if(r1.getEventA().equals(p1.getEventA()) && r1.getEventB().equals(p1.getEventB())){
                        double s = r1.getSupport()*p1.getSupport();
                        EventPairSupport ep = new EventPairSupport(r1.getEventA(),r1.getEventB(),s);
                        succession.add(ep);
                        break;
                    }
                }
            }
            setResults(succession,"succession");
            //handle no succession
            List<Tuple2<String, EventPairSupport>> notSuccession = intermediate
                    .filter(x -> x._2.getSupport() <= (1-bSupport.getValue()))
                    .collect();
            List<EventPairSupport> notSuccessionR = notSuccession.stream().filter(x -> x._1.equals("r"))
                    .map(x -> x._2).collect(Collectors.toList());
            List<EventPairSupport> notSuccessionP = notSuccession.stream().filter(x -> x._1.equals("p"))
                    .map(x -> x._2).collect(Collectors.toList());
            List<EventPairSupport> notSuccessionList = new ArrayList<>();
            for(EventPairSupport r1:notSuccessionR){
                for(EventPairSupport p1: notSuccessionP){
                    if(r1.getEventA().equals(p1.getEventA()) && r1.getEventB().equals(p1.getEventB())){
                        double s = (1-r1.getSupport())*(1-p1.getSupport());
                        EventPairSupport ep = new EventPairSupport(r1.getEventA(),r1.getEventB(),s);
                        notSuccessionList.add(ep);
                        break;
                    }
                }
            }
            setResults(notSuccessionList,"not-succession");
        }else if (precedence.isEmpty()){
            setResults(responses, "response");
        }else{
            setResults(precedence, "precedence");
        }
        intermediate.unpersist();
    }


    protected void setResults(List<EventPairSupport> results, String constraint) {
        switch (constraint) {
            case "response":
                this.queryResponseOrderedRelations.setResponse(results);
                break;
            case "precedence":
                this.queryResponseOrderedRelations.setPrecedence(results);
                break;
            case "succession":
                this.queryResponseOrderedRelations.setSuccession(results);
                break;
            case "not-succession":
                this.queryResponseOrderedRelations.setNotSuccession(results);
                break;
        }
    }


    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public QueryResponseOrderedRelations getQueryResponseOrderedRelations() {
        return queryResponseOrderedRelations;
    }
}
