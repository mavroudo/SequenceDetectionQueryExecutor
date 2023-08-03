package com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseOrderedRelations;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

import java.util.Set;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueryPlanOrderedRelationsChain extends QueryPlanOrderedRelations{

    public QueryPlanOrderedRelationsChain(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext, OrderedRelationsUtilityFunctions utils) {
        super(declareDBConnector, javaSparkContext, utils);
    }

    @Override
    public void initQueryResponse() {
        this.queryResponseOrderedRelations = new QueryResponseOrderedRelations("chain");
    }

    @Override
    public JavaRDD<Tuple4<String, String, String, Integer>> evaluateConstraint(JavaRDD<Tuple5<String, String, Long, Set<Integer>, Set<Integer>>> joined, String constraint) {
        JavaRDD<Tuple4<String, String, String, Integer>> tuple4JavaRDD;
        switch (constraint) {
            case "response":
                tuple4JavaRDD = joined.map(utils::countResponseChain);
                break;
            case "precedence":
                tuple4JavaRDD = joined.map(utils::countPrecedenceChain);
                break;
            default:
                tuple4JavaRDD =joined.map(utils::countPrecedenceChain).union(joined.map(utils::countResponseChain));
                break;
        }
        return tuple4JavaRDD
                .keyBy(y -> new Tuple3<>(y._1(), y._2(), y._3()))
                .reduceByKey((x, y) -> new Tuple4<>(x._1(), y._2(), y._3(), x._4() + y._4()))
                .map(x -> x._2);
    }
}
