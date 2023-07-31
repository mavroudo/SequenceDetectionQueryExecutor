package com.datalab.siesta.queryprocessor.declare.queries;

import com.datalab.siesta.queryprocessor.declare.queryPlans.existence.QueryPlanExistences;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class QueryExistence {

    private QueryPlanExistences queryPlanExistences;

    @Autowired
    public QueryExistence(QueryPlanExistences queryPlanExistences) {
        this.queryPlanExistences = queryPlanExistences;
    }

    public QueryPlanExistences getQueryPlan(Metadata metadata){
        queryPlanExistences.setMetadata(metadata);
        return queryPlanExistences;
    }
}
