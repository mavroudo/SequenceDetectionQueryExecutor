package com.datalab.siesta.queryprocessor.declare.queries;

import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderRelationsState;
import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderedRelations;
import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderedRelationsAlternate;
import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderedRelationsChain;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryOrderRelationWrapper;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.Query;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Starts the query plans that compute the constraints for the order-relation templates
 * (simple, alternate, chain)
 */
@Service
public class QueryOrderedRelations implements Query {

    private QueryPlanOrderedRelations queryPlanOrderedRelations;
    private QueryPlanOrderedRelationsAlternate queryPlanOrderedRelationsAlternate;
    private QueryPlanOrderedRelationsChain queryPlanOrderedRelationsChain;
    private QueryPlanOrderRelationsState queryOrderedRelationsState;

    @Autowired
    public QueryOrderedRelations(@Qualifier("queryPlanOrderedRelations") QueryPlanOrderedRelations queryPlanOrderedRelations,
    @Qualifier("queryPlanOrderedRelationsAlternate") QueryPlanOrderedRelationsAlternate queryPlanOrderedRelationsAlternate,
    @Qualifier("queryPlanOrderedRelationsChain") QueryPlanOrderedRelationsChain queryPlanOrderedRelationsChain,
    @Qualifier("queryPlanOrderRelationsState") QueryPlanOrderRelationsState queryPlanOrderReationsState) {
        this.queryPlanOrderedRelations = queryPlanOrderedRelations;
        this.queryPlanOrderedRelationsAlternate=queryPlanOrderedRelationsAlternate;
        this.queryPlanOrderedRelationsChain=queryPlanOrderedRelationsChain;
        this.queryOrderedRelationsState=queryPlanOrderReationsState;
    }

    @Override
    public QueryPlan createQueryPlan(QueryWrapper qw, Metadata m) {
        QueryOrderRelationWrapper qpw = (QueryOrderRelationWrapper) qw;

        if(!qpw.isStateAvailable()||qpw.isEnforceNormalMining()){ //execute the normal extraction
            switch (qpw.getMode()) {
                case "chain":
                    this.queryPlanOrderedRelationsChain.setMetadata(m);
                    this.queryPlanOrderedRelationsChain.initQueryResponse();
                    return this.queryPlanOrderedRelationsChain;
                case "alternate":
                    this.queryPlanOrderedRelationsAlternate.setMetadata(m);
                    this.queryPlanOrderedRelationsAlternate.initQueryResponse();
                    return this.queryPlanOrderedRelationsAlternate;
                default:
                    this.queryPlanOrderedRelations.setMetadata(m);
                    this.queryPlanOrderedRelations.initQueryResponse();
                    return this.queryPlanOrderedRelations;
            }
        }else{ //utilize the built states
            queryOrderedRelationsState.setMetadata(m);
            return queryOrderedRelationsState;
        }



        
    }

}
