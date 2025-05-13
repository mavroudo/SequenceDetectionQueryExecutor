package com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.DeclareUtilities;
import com.datalab.siesta.queryprocessor.declare.model.*;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseOrderedRelations;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryOrderRelationWrapper;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;

import lombok.Getter;
import lombok.Setter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;
import java.util.stream.Collectors;

@Component
@RequestScope
public class QueryPlanOrderedRelations implements QueryPlan{

    @Setter
    protected Metadata metadata;
    protected DeclareDBConnector declareDBConnector;
    protected JavaSparkContext javaSparkContext;
    @Getter
    protected QueryResponseOrderedRelations queryResponseOrderedRelations;
    protected OrderedRelationsUtilityFunctions utils;
    protected DeclareUtilities declareUtilities;

    @Autowired
    public QueryPlanOrderedRelations(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext,
                                     OrderedRelationsUtilityFunctions utils, DeclareUtilities declareUtilities) {
        this.declareDBConnector = declareDBConnector;
        this.javaSparkContext = javaSparkContext;
        this.utils = utils;
        this.declareUtilities=declareUtilities;
    }

    public void initQueryResponse() {
        this.queryResponseOrderedRelations = new QueryResponseOrderedRelations("simple");
    }


    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryOrderRelationWrapper qpw = (QueryOrderRelationWrapper) qw;
         //query IndexTable
         JavaRDD<EventPairToTrace> indexRDD = declareDBConnector.queryIndexOriginalDeclare(this.metadata.getLogname())
         .filter(x -> !x.getEventA().equals(x.getEventB()));
        //query SingleTable
        JavaPairRDD<Tuple2<String, String>, List<Integer>> singleRDD = declareDBConnector
                .querySingleTableAllDeclare(this.metadata.getLogname());

        //join tables using joinTables and flat map to get the single events
        JavaRDD<EventPairTraceOccurrences> joined = joinTables(indexRDD, singleRDD);
        joined.persist(StorageLevel.MEMORY_AND_DISK());

        //count the occurrences using the evaluate constraints
        JavaRDD<Abstract2OrderConstraint> c = evaluateConstraint(joined, qpw.getConstraint());
        //filter based on the values of the SingleTable and the provided support and write to the response
        Map<String, Long> uEventType = declareDBConnector.querySingleTableDeclare(this.metadata.getLogname())
                .map(x -> {
                    long all = x.getOccurrences().stream().mapToLong(OccurrencesPerTrace::getOccurrences).sum();
                    return new Tuple2<>(x.getEventType(), all);
                }).keyBy(x -> x._1).mapValues(x -> x._2).collectAsMap();
        Broadcast<Map<String, Long>> bUEventTypes = javaSparkContext.broadcast(uEventType);

        //add the not-succession constraints detected from the event pairs that did not occur in the database log
        extendNotSuccession(bUEventTypes.getValue(), this.metadata.getLogname(), c);
        //filter based on the user defined support
        filterBasedOnSupport(c, bUEventTypes, qpw.getSupport());
        joined.unpersist();
        return this.queryResponseOrderedRelations;
    }

    /**
     * for the traces that contain an occurrence of the event pair (a,b), joins their occurrences of these
     * event types (extracted from the Single Table).
     *
     * @param indexRDD  a rdd of {@link EventPairToTrace}
     * @param singleRDD a rdd of records that have the format (event
     * @ a rdd of {@link EventPairTraceOccurrences}
     */
    public JavaRDD<EventPairTraceOccurrences> joinTables(JavaRDD<EventPairToTrace> indexRDD,
                                                         JavaPairRDD<Tuple2<String, String>, List<Integer>> singleRDD) {
        //for the traces that contain an occurrence of the event pair (a,b), joins their occurrences of these
        //event types (extracted from the Single Table)
        return indexRDD
                .keyBy(r -> new Tuple2<>(r.getEventA(), r.getTrace_id()))
                .join(singleRDD)
                .map(x -> x._2)
                //join based on the second event
                .keyBy(x -> new Tuple2<>(x._1.getEventB(), x._1.getTrace_id()))
                .join(singleRDD)
                .map(x -> {
                    String eventA = x._2._1._1.getEventA();//event a
                    String eventB = x._2._1._1.getEventB();//event b
                    List<Integer> f = x._2._1._2; // occurrences of the first event
                    List<Integer> s = x._2._2;//occurrences of the second event
                    String tid = x._1._2; //trace id
                    return new EventPairTraceOccurrences(eventA, eventB, tid, f, s);
                });
    }

    /**
     * Calculates the number of occurrences for each pattern per trace, depending on the different
     * constraint type.
     *
     * @param joined     a rdd of {@link EventPairTraceOccurrences}
     * @param constraint a string that describes the constraint under evaluation 'response', 'precedence'
     *                   or 'succession' (which is the default execution)
     * @return a rdd of {@link  Abstract2OrderConstraint}
     */
    public JavaRDD<Abstract2OrderConstraint> evaluateConstraint
    (JavaRDD<EventPairTraceOccurrences> joined, String constraint) {

        JavaRDD<Abstract2OrderConstraint> tuple4JavaRDD;
        switch (constraint) {
            case "response":
                tuple4JavaRDD = joined.map(utils::countResponse);
                break;
            case "precedence":
                tuple4JavaRDD = joined.map(utils::countPrecedence);
                break;
            default:
                tuple4JavaRDD = joined.map(utils::countPrecedence).union(joined.map(utils::countResponse));
                break;
        }
        return tuple4JavaRDD
                //reduce by (eventA, eventB, mode - r/p)
                .keyBy(y -> new Tuple3<>(y.getEventA(), y.getEventB(), y.getMode()))
                .reduceByKey((x, y) -> {
                    x.setOccurrences(x.getOccurrences() + y.getOccurrences());
                    return x;
                })
                .map(x -> x._2);

    }


    /**
     * filters based on support and constraint required and write them to the response
     *
     * @param c the occurrences of different templates detected
     * @param bUEventType a spark broadcast map fo the form (event type) -> total occurrences in the log database
     * @param support the user-defined support
     */
    public void filterBasedOnSupport(JavaRDD<Abstract2OrderConstraint> c,
                                     Broadcast<Map<String, Long>> bUEventType, double support) {
        Broadcast<Double> bSupport = javaSparkContext.broadcast(support);
        //calculates the support based on either the total occurrences of the first event (response)
        //or the occurrences of the second event (precedence)
        JavaRDD<Tuple2<String, EventPairSupport>> intermediate = c.map(x -> {
            long total;
            if (x.getMode().equals("r")) { //response
                total = bUEventType.getValue().get(x.getEventA());
            } else {
                total = bUEventType.getValue().get(x.getEventB());
            }
            long found = x.getOccurrences();
            return new Tuple2<>(x.getMode(), new EventPairSupport(x.getEventA(), x.getEventB(), (double) found / total));
        });
        intermediate.persist(StorageLevel.MEMORY_AND_DISK());
        //filters based on the user-defined support and collects the result
        List<Tuple2<String, EventPairSupport>> detected = intermediate
                .filter(x -> x._2.getSupport() >= bSupport.getValue())
                .collect();
        //splits the patterns that correspond to response and precedence into two separete lists
        List<EventPairSupport> responses = detected.stream().filter(x -> x._1.equals("r"))
                .map(x -> x._2).collect(Collectors.toList());
        List<EventPairSupport> precedence = detected.stream().filter(x -> x._1.equals("p"))
                .map(x -> x._2).collect(Collectors.toList());

        if (!precedence.isEmpty() && !responses.isEmpty()) { //we are looking for succession and no succession
            setResults(responses, "response");
            setResults(precedence, "precedence");
            //handle succession (both "r" and "p" of these events should have a support greater than the user-defined
            List<EventPairSupport> succession = new ArrayList<>();
            for (EventPairSupport r1 : responses) {
                for (EventPairSupport p1 : precedence) {
                    if (r1.getEventA().equals(p1.getEventA()) && r1.getEventB().equals(p1.getEventB())) {
                        double s = r1.getSupport() * p1.getSupport();
                        EventPairSupport ep = new EventPairSupport(r1.getEventA(), r1.getEventB(), s);
                        succession.add(ep);
                        break;
                    }
                }
            }
            setResults(succession, "succession");
            //handle no succession
            //event pairs where both "r" and "p" have support less than the user-defined
            //for the pairs that do not appear here the function extendNotSuccession() is executed - check below
            List<Tuple2<String, EventPairSupport>> notSuccession = intermediate
                    .filter(x -> x._2.getSupport() <= (1 - bSupport.getValue()))
                    .collect();
            List<EventPairSupport> notSuccessionR = notSuccession.stream().filter(x -> x._1.equals("r"))
                    .map(x -> x._2).collect(Collectors.toList());
            List<EventPairSupport> notSuccessionP = notSuccession.stream().filter(x -> x._1.equals("p"))
                    .map(x -> x._2).collect(Collectors.toList());
            //create the event pair support based on the above event pairs
            List<EventPairSupport> notSuccessionList = new ArrayList<>();
            for (EventPairSupport r1 : notSuccessionR) {
                for (EventPairSupport p1 : notSuccessionP) {
                    if (r1.getEventA().equals(p1.getEventA()) && r1.getEventB().equals(p1.getEventB())) {
                        double s = (1 - r1.getSupport()) * (1 - p1.getSupport());
                        EventPairSupport ep = new EventPairSupport(r1.getEventA(), r1.getEventB(), s);
                        notSuccessionList.add(ep);
                        break;
                    }
                }
            }
            //set the different lists to the appropriate lists
            setResults(notSuccessionList, "not-succession");
        } else if (precedence.isEmpty()) {
            setResults(responses, "response");
        } else {
            setResults(precedence, "precedence");
        }
        intermediate.unpersist();
    }


    /**
     * Sets the different list of constraints to the appropriate fields in the response
     *
     * @param results    a list containing detected constraints
     * @param constraint the mode (response/precedence/succession/not-succession)
     */
    protected void setResults(List<EventPairSupport> results, String constraint) {
        switch (constraint) {
            case "response":
                queryResponseOrderedRelations.setResponse(results);
                break;
            case "precedence":
                queryResponseOrderedRelations.setPrecedence(results);
                break;
            case "succession":
                queryResponseOrderedRelations.setSuccession(results);
                break;
            case "not-succession":
                queryResponseOrderedRelations.setNotSuccession(results);
                break;
        }
    }

    /**
     * Detects not succession patterns from the not found event pairs
     *
     * @param uEventType a map of the form (event type) -> total occurrences in the database log
     * @param logname    the name of the log database (used to load information from the database)
     * @param cSimple    the extracted constraints, a rdd of {@link Abstract2OrderConstraint}
     */
    public void extendNotSuccession(Map<String, Long> uEventType, String logname,
                                    JavaRDD<Abstract2OrderConstraint> cSimple) {
        // since the first argument may not be available it will be loaded and calculated from the database
        if (uEventType == null) {
            uEventType = declareDBConnector.extractTotalOccurrencesPerEventType(logname);
        }
        //transform rdd to a compatible version to be used by the extractNotFoundPairs
        JavaRDD<EventPairToNumberOfTrace> mappedRdd = cSimple
                .map(x->new EventPairToNumberOfTrace(x.getEventA(),x.getEventB(),1));
        Set<EventPair> notFound = declareUtilities.extractNotFoundPairs(uEventType.keySet(),mappedRdd);
        //transform event pairs to event pairs with support (which is 100% by definition)
        List<EventPairSupport> result = notFound.stream().map(x -> new EventPairSupport(x.getEventA().getName(),
                x.getEventB().getName(), 1)).collect(Collectors.toList());
        this.queryResponseOrderedRelations.setNotSuccession(result);
    }



}
