package com.datalab.siesta.queryprocessor.model.Queries.QueryTypes;

import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlanPatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class QueryPatternDetection implements Query{

    private QueryPlanPatternDetection qppd;

    @Autowired
    public QueryPatternDetection(QueryPlanPatternDetection qppd){
        this.qppd=qppd;
    }

    @Override
    public QueryPlan createQueryPlan(QueryWrapper qw, Metadata m) {
        qppd.setMetadata(m);
        //TODO: implement logic that will determine the different query plans
        return qppd;
    }
}
