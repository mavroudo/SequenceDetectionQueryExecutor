package com.datalab.siesta.queryprocessor.model.Queries.QueryTypes;

import com.datalab.siesta.queryprocessor.model.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlanPatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class QueryPatternDetection implements Query{

    @Autowired
    private QueryPlanPatternDetection qppd;

    @Override
    public QueryPlan createQueryPlan(QueryWrapper qw, Metadata m) {
        //TODO: implement logic that will determine the different query plans
        return qppd;
    }
}
