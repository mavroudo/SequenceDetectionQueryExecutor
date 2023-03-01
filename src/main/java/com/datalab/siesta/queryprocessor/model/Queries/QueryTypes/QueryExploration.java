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

@Service
public class QueryExploration implements Query{

    private QueryPlanExplorationFast queryPlanExplorationFast;
    private QueryPlanExplorationAccurate queryPlanExplorationAccurate;
    private QueryPlanExplorationHybrid queryPlanExplorationHybrid;

    @Autowired
    public QueryExploration(QueryPlanExplorationFast queryPlanExplorationFast,
                            QueryPlanExplorationAccurate queryPlanExplorationAccurate,
                            QueryPlanExplorationHybrid queryPlanExplorationHybrid) {
        this.queryPlanExplorationFast = queryPlanExplorationFast;
        this.queryPlanExplorationAccurate = queryPlanExplorationAccurate;
        this.queryPlanExplorationHybrid = queryPlanExplorationHybrid;
    }

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
