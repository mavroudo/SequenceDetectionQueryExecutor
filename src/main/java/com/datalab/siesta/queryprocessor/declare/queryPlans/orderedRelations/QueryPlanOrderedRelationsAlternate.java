package com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.model.EventPairSupport;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseOrderedRelations;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueryPlanOrderedRelationsAlternate extends QueryPlanOrderedRelations{
    public QueryPlanOrderedRelationsAlternate(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext,
                                              OrderedRelationsUtilityFunctions utils) {
        super(declareDBConnector, javaSparkContext, utils);
    }

    @Override
    public void initQueryResponse() {
        this.queryResponseOrderedRelations = new QueryResponseOrderedRelations("alternate");
    }

    @Override
    protected JavaRDD<Tuple4<String, String, String, Integer>> evaluateConstraint(JavaRDD<Tuple5<String, String, Long, Set<Integer>, Set<Integer>>> joined, String constraint) {
        JavaRDD<Tuple4<String, String, String, Integer>> tuple4JavaRDD;
        switch (constraint) {
            case "response":
                tuple4JavaRDD = joined.map(utils::countResponseAlternate);
                break;
            case "precedence":
                tuple4JavaRDD = joined.map(utils::countPrecedenceAlternate);
                break;
            default:
                tuple4JavaRDD =joined.map(utils::countPrecedenceAlternate).union(joined.map(utils::countResponseAlternate));
                break;
        }
        return tuple4JavaRDD
                .keyBy(y -> new Tuple3<>(y._1(), y._2(), y._3()))
                .reduceByKey((x, y) -> new Tuple4<>(x._1(), y._2(), y._3(), x._4() + y._4()))
                .map(x -> x._2);
    }

    @Override
    protected void filterBasedOnSupport(JavaRDD<Tuple4<String, String, String, Integer>> c,
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
        }else if (precedence.isEmpty()){
            setResults(responses, "response");
        }else{
            setResults(precedence, "precedence");
        }
        intermediate.unpersist();
    }
}
