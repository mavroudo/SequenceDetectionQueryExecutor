package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans;

import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;


/**
 * Interface that describes the execution plan. Each Query type, based on the input parameter and the underlying
 * data will choose a query plan to be executed. This class contains two functions, the first one is to set the metadata
 * of the log database and the second one is the execute (which executes the query and returns the query response).
 * The flow is: Endpoint (query wrapper)-> QueryTypes -> creates a QueryPlan -> the QueryPlan is executed -> a QueryResponse is returned
 */
@Component
@RequestScope
public interface QueryPlan {
    QueryResponse execute(QueryWrapper qw);

    void setMetadata(Metadata metadata);
}
