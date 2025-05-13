package com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.DeclareUtilities;
import com.datalab.siesta.queryprocessor.declare.model.Abstract2OrderConstraint;
import com.datalab.siesta.queryprocessor.declare.model.EventPairTraceOccurrences;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseOrderedRelations;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;
import scala.Tuple3;


@Component
@RequestScope
public class QueryPlanOrderedRelationsChain extends QueryPlanOrderedRelations{

    public QueryPlanOrderedRelationsChain(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext,
                                          OrderedRelationsUtilityFunctions utils, DeclareUtilities declareUtilities) {
        super(declareDBConnector, javaSparkContext, utils,declareUtilities);
    }

    @Override
    public void initQueryResponse() {
        this.queryResponseOrderedRelations = new QueryResponseOrderedRelations("chain");
    }

    @Override
    public JavaRDD<Abstract2OrderConstraint> evaluateConstraint(JavaRDD<EventPairTraceOccurrences> joined,
                                                                String constraint) {
        JavaRDD<Abstract2OrderConstraint> tuple4JavaRDD;
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
                .keyBy(y -> new Tuple3<>(y.getEventA(), y.getEventB(), y.getMode()))
                .reduceByKey((x, y) -> {
                    x.setOccurrences(x.getOccurrences() + y.getOccurrences());
                    return x;
                })
                .map(x -> x._2);
    }
}
