package com.datalab.siesta.queryprocessor.model.Queries.QueryTypes;

import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.ExtractedPairsForPatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.Detection.QueryPlanPatternDetectionSingle;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.Detection.QueryPlanPatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.Detection.QueryPlanPatternDetectionGroups;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.Detection.QueryPlanWhyNotMatch;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Pattern Detection: Based on the characteristics of the query pattern this class determines which query plan will
 * be executed. If the pattern has groups defined then the QueryPlanPatternDetectionGroups will be executed. If
 * the patterns has the explainability setting on then the QueryPlanWhyNotMatch will be executed. If none of the above is
 * correct then the generic QueryPlanPatternDetection will be executed.
 */
@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueryPatternDetection implements Query {

    private final QueryPlanPatternDetection qppd;
    private final QueryPlanPatternDetectionGroups qppdg;
    private final QueryPlanWhyNotMatch planWhyNotMatch;
    private final QueryPlanPatternDetectionSingle qpds;

    @Autowired
    public QueryPatternDetection(@Qualifier("queryPlanPatternDetection") QueryPlanPatternDetection qppd,
                                 @Qualifier("queryPlanPatternDetectionGroups") QueryPlanPatternDetectionGroups qppdg,
                                 @Qualifier("queryPlanPatternDetectionSingle") QueryPlanPatternDetectionSingle qpds,
                                 QueryPlanWhyNotMatch planWhyNotMatch) {
        this.qppd = qppd;
        this.qppdg = qppdg;
        this.qpds = qpds;
        this.planWhyNotMatch = planWhyNotMatch;
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

        boolean fromOrTillSet = qpdw.getFrom() != null || qpdw.getTill() != null;
        List<ExtractedPairsForPatternDetection> pairs = qpdw.getPattern().extractPairsForPatternDetection(fromOrTillSet);

        if(pairs.isEmpty() || pairs.get(0).getTruePairs().isEmpty()){ //either is empty or there is no true pairs
            qpds.setEventTypesInLog(qpdw.getPattern().getEventTypes());
            qpds.setMetadata(m);
            return qpds;
        }

        if (qpdw.isWhyNotMatchFlag()) {
            planWhyNotMatch.setMetadata(m);
            planWhyNotMatch.setEventTypesInLog(qpdw.getPattern().getEventTypes());
            return planWhyNotMatch;
        } else if (qpdw.isHasGroups()) {
            qppdg.setMetadata(m);
            qppdg.setEventTypesInLog(qpdw.getPattern().getEventTypes());
            return qppdg;
        } else {
            qppd.setEventTypesInLog(qpdw.getPattern().getEventTypes());
            qppd.setMetadata(m);
            return qppd;
        }
    }
}
