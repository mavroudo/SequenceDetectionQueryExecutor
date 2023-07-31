package com.datalab.siesta.queryprocessor.declare.queries;

import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderedRelations;
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

}
