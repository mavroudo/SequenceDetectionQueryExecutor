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

/**
 * Pattern Detection: Based on the characteristics of the query pattern this class determines which query plan will
 * be executed. If the pattern has groups defined then the QueryPlanPatternDetectionGroups will be executed. If
 * the patterns has the explainability setting on then the QueryPlanWhyNotMatch will be executed. If none of the above is
 * correct then the generic QueryPlanPatternDetection will be executed.
 */
@Service
public class QueryPatternDetection implements Query{

    private final QueryPlanPatternDetection qppd;
    private final QueryPlanWhyNotMatch planWhyNotMatch;

    @Autowired
    public QueryPatternDetection(@Qualifier("queryPlanPatternDetection") QueryPlanPatternDetection qppd,
                                 QueryPlanWhyNotMatch planWhyNotMatch){
        this.qppd=qppd;
        this.planWhyNotMatch=planWhyNotMatch;
    }

    /**
     * Determines which query plan will be executed. If the pattern has groups defined then the
     * QueryPlanPatternDetectionGroups will be executed. If the patterns has the explainability setting on then the
     * QueryPlanWhyNotMatch will be executed. If none of the above is
     * correct then the generic QueryPlanPatternDetection will be executed.
     */

    @Override
    public QueryPlan createQueryPlan(QueryWrapper qw, Metadata m) {
        QueryPatternDetectionWrapper qpdw = (QueryPatternDetectionWrapper) qw;

        if(qpdw.isWhyNotMatchFlag()){
            planWhyNotMatch.setMetadata(m);
            planWhyNotMatch.setEventTypesInLog(qpdw.getPattern().getEventTypes());
            return planWhyNotMatch;
        }else {
            qppd.setEventTypesInLog(qpdw.getPattern().getEventTypes());
            qppd.setMetadata(m);
            return qppd;
        }
    }
}
