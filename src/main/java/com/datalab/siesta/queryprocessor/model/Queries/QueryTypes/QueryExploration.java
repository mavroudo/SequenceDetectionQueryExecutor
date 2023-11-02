package com.datalab.siesta.queryprocessor.model.Queries.QueryTypes;

import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlanExplorationAccurate;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlanExplorationFast;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlanExplorationHybrid;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryExploreWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Exploration Query. Based on the mode set in the wrapper (fast/accurate/hybrid) it returns the appropriate
 * query plan for execution.
 */
@Service
public class QueryExploration implements Query{

    private final QueryPlanExplorationFast queryPlanExplorationFast;
    private final QueryPlanExplorationAccurate queryPlanExplorationAccurate;
    private final QueryPlanExplorationHybrid queryPlanExplorationHybrid;

    @Autowired
    public QueryExploration(QueryPlanExplorationFast queryPlanExplorationFast,
                            QueryPlanExplorationAccurate queryPlanExplorationAccurate,
                            QueryPlanExplorationHybrid queryPlanExplorationHybrid) {
        this.queryPlanExplorationFast = queryPlanExplorationFast;
        this.queryPlanExplorationAccurate = queryPlanExplorationAccurate;
        this.queryPlanExplorationHybrid = queryPlanExplorationHybrid;
    }

    /**
     * Based on the mode set in the wrapper (fast/accurate/hybrid) it returns the appropriate
     *  query plan for execution.
     * @param qw the query wrapper
     * @param m the metadata of the log database
     * @return a query plan based on the provided mode (Default = fast)
     */
    @Override
    public QueryPlan createQueryPlan(QueryWrapper qw, Metadata m) {
        QueryExploreWrapper queryExploreWrapper = (QueryExploreWrapper) qw;
        if(queryExploreWrapper.getMode().equals("accurate")){
            queryPlanExplorationAccurate.setMetadata(m);
            return queryPlanExplorationAccurate;
        }else if(queryExploreWrapper.getMode().equals("hybrid")) {
            queryPlanExplorationHybrid.setMetadata(m);
            return queryPlanExplorationHybrid;
        }else{
            queryPlanExplorationFast.setMetadata(m);
            return queryPlanExplorationFast;
        }
    }
}
