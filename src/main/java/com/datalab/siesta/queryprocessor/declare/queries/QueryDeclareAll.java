package com.datalab.siesta.queryprocessor.declare.queries;

import org.springframework.beans.factory.annotation.Autowired;

import com.datalab.siesta.queryprocessor.declare.queryPlans.QueryPlanDeclareAll;
import com.datalab.siesta.queryprocessor.declare.queryPlans.QueryPlanDeclareAllState;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryWrapperDeclare;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.Query;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import org.springframework.stereotype.Service;

@Service
public class QueryDeclareAll implements Query{

    private QueryPlanDeclareAll queryPlanDeclareAll;
    private QueryPlanDeclareAllState queryPlanDeclareAllState;

    @Autowired
    public QueryDeclareAll(QueryPlanDeclareAll queryPlanDeclareAll, QueryPlanDeclareAllState queryPlanDeclareAllState) {
        this.queryPlanDeclareAll = queryPlanDeclareAll;
        this.queryPlanDeclareAllState = queryPlanDeclareAllState;
    }

    @Override
    public QueryPlan createQueryPlan(QueryWrapper qw, Metadata m) {
        QueryWrapperDeclare qpw = (QueryWrapperDeclare) qw;
        if(!qpw.isStateAvailable()||qpw.isEnforceNormalMining()){ //execute the normal extraction
            queryPlanDeclareAll.setMetadata(m);
            return queryPlanDeclareAll;
        }else{ //utilize the built states
            queryPlanDeclareAllState.setMetadata(m);
            return queryPlanDeclareAllState;
        }
    }



}
