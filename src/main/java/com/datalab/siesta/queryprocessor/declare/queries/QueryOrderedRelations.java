package com.datalab.siesta.queryprocessor.declare.queries;

import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderedRelations;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Service
public class QueryOrderedRelations {

    private QueryPlanOrderedRelations queryPlanOrderedRelations;

    @Autowired
    public QueryOrderedRelations(QueryPlanOrderedRelations queryPlanOrderedRelations) {
        this.queryPlanOrderedRelations = queryPlanOrderedRelations;
    }

    public QueryPlanOrderedRelations getQueryPlan(Metadata metadata, String mode){
        this.queryPlanOrderedRelations.setMetadata(metadata);
        this.queryPlanOrderedRelations.initQueryResponse();
        return this.queryPlanOrderedRelations;
    }

}
