package com.datalab.siesta.queryprocessor.declare.queryPlans.position;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponsePosition;
import com.datalab.siesta.queryprocessor.model.DBModel.Trace;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;
import scala.Tuple2;

@Component
@RequestScope
public class QueryPlanFirst extends QueryPlanPosition{


    public QueryPlanFirst(){
        super();
    }
    @Autowired
    public QueryPlanFirst(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext){
        super(declareDBConnector,javaSparkContext);
    }

    @Override
    protected JavaRDD<Tuple2<String, Integer>> getTracesInPosition(JavaRDD<Trace> traces) {
        return traces.map(x-> new Tuple2<>(x.getEvents().get(0).getName(),1));
    }

    @Override
    public QueryResponse execute(String log_database, double support) {
        QueryResponsePosition queryResponsePosition = (QueryResponsePosition) super.execute(log_database, support);
        queryResponsePosition.setFirstTuple(results);
        return queryResponsePosition;
    }
}
