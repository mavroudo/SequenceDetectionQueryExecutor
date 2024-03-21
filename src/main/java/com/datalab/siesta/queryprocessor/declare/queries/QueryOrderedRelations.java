package com.datalab.siesta.queryprocessor.declare.queries;

import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderedRelations;
import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderedRelationsAlternate;
import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderedRelationsChain;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Starts the query plans that compute the constraints for the order-relation templates
 * (simple, alternate, chain)
 */
@Service
public class QueryOrderedRelations {

    private QueryPlanOrderedRelations queryPlanOrderedRelations;
    private QueryPlanOrderedRelationsAlternate queryPlanOrderedRelationsAlternate;
    private QueryPlanOrderedRelationsChain queryPlanOrderedRelationsChain;

    @Autowired
    public QueryOrderedRelations(QueryPlanOrderedRelations queryPlanOrderedRelations,
                                 QueryPlanOrderedRelationsAlternate queryPlanOrderedRelationsAlternate,
                                 QueryPlanOrderedRelationsChain queryPlanOrderedRelationsChain) {
        this.queryPlanOrderedRelations = queryPlanOrderedRelations;
        this.queryPlanOrderedRelationsAlternate=queryPlanOrderedRelationsAlternate;
        this.queryPlanOrderedRelationsChain=queryPlanOrderedRelationsChain;
    }

    public QueryPlanOrderedRelations getQueryPlan(Metadata metadata, String mode) {

        switch (mode) {
            case "chain":
                this.queryPlanOrderedRelationsChain.setMetadata(metadata);
                this.queryPlanOrderedRelationsChain.initQueryResponse();
                return this.queryPlanOrderedRelationsChain;
            case "alternate":
                this.queryPlanOrderedRelationsAlternate.setMetadata(metadata);
                this.queryPlanOrderedRelationsAlternate.initQueryResponse();
                return this.queryPlanOrderedRelationsAlternate;
            default:
                this.queryPlanOrderedRelations.setMetadata(metadata);
                this.queryPlanOrderedRelations.initQueryResponse();
                return this.queryPlanOrderedRelations;

        }

    }

}
