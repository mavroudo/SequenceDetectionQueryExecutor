package com.datalab.siesta.queryprocessor.declare.queries;
import com.datalab.siesta.queryprocessor.declare.queryPlans.position.QueryPlanPositions;
import com.datalab.siesta.queryprocessor.declare.queryPlans.position.QueryPlanPositionsState;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryPositionWrapper;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.Query;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 *
 * Starts the query plans that compute the constraints for the position templates
 * (first, last, both)
 *
 */
@Service
public class QueryPositions implements Query{

    private QueryPlanPositions queryPlanPositions;

    private QueryPlanPositionsState queryPlanPositionsState;


    @Autowired
    public QueryPositions(QueryPlanPositions queryPlanPositions, QueryPlanPositionsState queryPlanPositionsState) {
        this.queryPlanPositions = queryPlanPositions;
        this.queryPlanPositionsState = queryPlanPositionsState;
    }

    @Override
    public QueryPlan createQueryPlan(QueryWrapper qw, Metadata m) {
        QueryPositionWrapper qpw = (QueryPositionWrapper) qw;
        if(!qpw.isStateAvailable()||qpw.isEnforceNormalMining()){ //execute the normal extraction
            queryPlanPositions.setMetadata(m);
            return queryPlanPositions;
        }else{ //utilize the built states
            queryPlanPositionsState.setMetadata(m);
            return queryPlanPositionsState;
        }

    }
}
