package com.datalab.siesta.queryprocessor.declare.queries;

import com.datalab.siesta.queryprocessor.declare.queryPlans.existence.QueryPlanExistancesState;
import com.datalab.siesta.queryprocessor.declare.queryPlans.existence.QueryPlanExistences;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryExistenceWrapper;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.Query;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Starts the query plan that computes the constraints for all the existence templates
 */
@Service
public class QueryExistence implements Query{

    private QueryPlanExistences queryPlanExistences;
    private QueryPlanExistancesState queryPlanExistancesState;

    @Autowired
    public QueryExistence(QueryPlanExistences queryPlanExistences, QueryPlanExistancesState queryPlanExistancesState) {
        this.queryPlanExistences = queryPlanExistences;
        this.queryPlanExistancesState = queryPlanExistancesState;
    }

    @Override
    public QueryPlan createQueryPlan(QueryWrapper qw, Metadata m) {

        QueryExistenceWrapper qpw = (QueryExistenceWrapper) qw;
        if(!qpw.isStateAvailable()||qpw.isEnforceNormalMining()){ //execute the normal extraction
            queryPlanExistences.setMetadata(m);
            return queryPlanExistences;
        }else{ //utilize the built states
            queryPlanExistancesState.setMetadata(m);
            return queryPlanExistancesState;
        }
        
    }
}
