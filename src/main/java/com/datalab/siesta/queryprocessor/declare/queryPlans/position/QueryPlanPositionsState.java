package com.datalab.siesta.queryprocessor.declare.queryPlans.position;

import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.queryPlans.QueryPlanState;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;

@Component
@RequestScope
public class QueryPlanPositionsState extends QueryPlanState {

    public QueryPlanPositionsState(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext){
        super(declareDBConnector,javaSparkContext);
    }


    @Override
    public QueryResponse execute(QueryWrapper qw) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'execute'");
    }

    /**
     * Extracts statistics about events and traces indexed in the declare states
     */
    protected void extractStatistics(){
        //TODO: implement them using the declareDBConnector
    }

    

}
