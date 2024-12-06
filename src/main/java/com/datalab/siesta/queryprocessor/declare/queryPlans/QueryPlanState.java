package com.datalab.siesta.queryprocessor.declare.queryPlans;

import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;

@Component
@RequestScope
public class QueryPlanState  implements QueryPlan{

    /**
     * Connection with the database
     */
    protected final DeclareDBConnector declareDBConnector;

    /**
     * Connection with the spark
     */
    protected JavaSparkContext javaSparkContext;

    /**
     * Log Database's metadata
     */
    protected Metadata metadata;

    public QueryPlanState(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext) {
        this.declareDBConnector = declareDBConnector;
        this.javaSparkContext = javaSparkContext;
    }

    @Override
    public QueryResponse execute(QueryWrapper qw) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'execute'");
    }

    @Override
    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

}
