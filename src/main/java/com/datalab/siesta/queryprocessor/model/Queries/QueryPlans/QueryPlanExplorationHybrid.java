package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseConnector;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Patterns.SimplePattern;
import com.datalab.siesta.queryprocessor.model.Proposition;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseExploration;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryExploreWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The query plan for the accurate detection of continuation for the query pattern
 */
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueryPlanExplorationHybrid extends QueryPlanExplorationAccurate {

    private final QueryPlanExplorationFast queryPlanExplorationFast;

    @Autowired
    public QueryPlanExplorationHybrid(DBConnector dbConnector, SaseConnector saseConnector, Utils utils,
                                      QueryPlanExplorationFast queryPlanExplorationFast) {
        super(dbConnector, saseConnector, utils);
        this.queryPlanExplorationFast = queryPlanExplorationFast;
    }

    /**
     * The hybrid explorations is a trade-off between accuracy and efficiency. The idea is that the fast exploration
     * is executed first and then for each of the top "k" propositions yielded by the exploration, a complete
     * pattern detection is executed. That way as the "k" increases the process becomes more time-consuming and comes
     * closer to the accurate method. On the other hand if "k" decreases the process becomes less accurate and resembles
     * the fast exploration
     * @param qw the QueryPatternDetectionWrapper
     * @return te possible next events sorted based on frequency, in the form of propositions
     */
    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryExploreWrapper queryExploreWrapper = (QueryExploreWrapper) qw;
        queryPlanExplorationFast.setMetadata(metadata);
        List<Proposition> fast = ((QueryResponseExploration) queryPlanExplorationFast.execute(qw)).getPropositions();
        List<Proposition> props = new ArrayList<>();
        for(Proposition p : fast.subList(0, queryExploreWrapper.getK())){
            try {
                SimplePattern sp = (SimplePattern) queryExploreWrapper.getPattern().clone();
                Proposition newp = this.patternDetection(sp,p.getEvent());
                if (newp != null) props.add(newp);
            }catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }
        props.sort(Collections.reverseOrder());
        return new QueryResponseExploration(props);
    }

    @Override
    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }
}
