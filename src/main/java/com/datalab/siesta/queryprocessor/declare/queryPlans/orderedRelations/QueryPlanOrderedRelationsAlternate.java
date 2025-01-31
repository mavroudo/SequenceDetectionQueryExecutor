package com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.DeclareUtilities;
import com.datalab.siesta.queryprocessor.declare.model.Abstract2OrderConstraint;
import com.datalab.siesta.queryprocessor.declare.model.EventPairSupport;
import com.datalab.siesta.queryprocessor.declare.model.EventPairTraceOccurrences;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseOrderedRelations;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;
import scala.Tuple2;
import scala.Tuple3;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@RequestScope
public class QueryPlanOrderedRelationsAlternate extends QueryPlanOrderedRelations {
    public QueryPlanOrderedRelationsAlternate(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext,
                                              OrderedRelationsUtilityFunctions utils, DeclareUtilities declareUtilities) {
        super(declareDBConnector, javaSparkContext, utils, declareUtilities);
    }

    @Override
    public void initQueryResponse() {
        this.queryResponseOrderedRelations = new QueryResponseOrderedRelations("alternate");
    }

    @Override
    public JavaRDD<Abstract2OrderConstraint> evaluateConstraint
            (JavaRDD<EventPairTraceOccurrences> joined, String constraint) {
        JavaRDD<Abstract2OrderConstraint> tuple4JavaRDD;
        switch (constraint) {
            case "response":
                tuple4JavaRDD = joined.map(utils::countResponseAlternate);
                break;
            case "precedence":
                tuple4JavaRDD = joined.map(utils::countPrecedenceAlternate);
                break;
            default:
                tuple4JavaRDD = joined.map(utils::countPrecedenceAlternate).union(joined.map(utils::countResponseAlternate));
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

    @Override
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
            return new Tuple2<>(x.getMode(), new EventPairSupport(x.getEventA(), x.getEventB(),
                    (double) found / total));
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
        } else if (precedence.isEmpty()) {
            setResults(responses, "response");
        } else {
            setResults(precedence, "precedence");
        }
        intermediate.unpersist();
    }
}
