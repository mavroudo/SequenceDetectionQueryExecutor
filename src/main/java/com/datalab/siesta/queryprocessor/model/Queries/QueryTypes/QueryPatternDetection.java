package com.datalab.siesta.queryprocessor.model.Queries.QueryTypes;

import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlanPatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlanWhyNotMatch;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class QueryPatternDetection implements Query{

    private QueryPlanPatternDetection qppd;
    private QueryPlanWhyNotMatch planWhyNotMatch;

    @Autowired
    public QueryPatternDetection(@Qualifier("queryPlanPatternDetection") QueryPlanPatternDetection qppd,
                                 QueryPlanWhyNotMatch planWhyNotMatch){
        this.qppd=qppd;
        this.planWhyNotMatch=planWhyNotMatch;
    }

    @Override
    public QueryPlan createQueryPlan(QueryWrapper qw, Metadata m) {
        QueryPatternDetectionWrapper qpdw = (QueryPatternDetectionWrapper) qw;

        if(qpdw.isWhyNotMatchFlag()){
            planWhyNotMatch.setMetadata(m);
//            int n = qpdw.getPattern().getSize();
            planWhyNotMatch.setEventTypesInLog(qpdw.getPattern().getEventTypes());
//            planWhyNotMatch.setMinPairs(((n-1)*(n-2))/2); //set min pairs
//            planWhyNotMatch.setMinPairs(1); //set min pairs
            return planWhyNotMatch;
        }else {
            qppd.setEventTypesInLog(qpdw.getPattern().getEventTypes());
            qppd.setMetadata(m);
            return qppd;
        }
    }
}
