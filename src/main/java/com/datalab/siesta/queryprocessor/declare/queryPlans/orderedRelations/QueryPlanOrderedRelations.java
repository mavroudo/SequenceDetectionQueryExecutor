package com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseOrderedRelations;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueryPlanOrderedRelations {

    protected Metadata metadata;
    protected DeclareDBConnector declareDBConnector;
    protected JavaSparkContext javaSparkContext;
    protected QueryResponseOrderedRelations queryResponseOrderedRelations;

    @Autowired
    public QueryPlanOrderedRelations(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext) {
        this.declareDBConnector = declareDBConnector;
        this.javaSparkContext = javaSparkContext;
        this.queryResponseOrderedRelations=new QueryResponseOrderedRelations("simple");
    }


    public QueryResponse execute(String logname, double support){
        QueryResponseOrderedRelations qr = new QueryResponseOrderedRelations("simple");
        //query IndexTable
        //join tables using joinTables
        //flat map to get the single events
        //count the occurrences using the evaluate constraints
        //filter based on the provided support
        return null;
    }

    protected void joinTables(String mode){

    }

    protected void evaluateConstraint(){

    }




    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }
}
