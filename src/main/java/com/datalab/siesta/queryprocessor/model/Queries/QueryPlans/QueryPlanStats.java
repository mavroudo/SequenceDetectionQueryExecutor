package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseStats;

import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryStatsWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

/**
 * Executing the stats query, which returns the basic statistics for each consecutive et-pair in the query pattern.
 * The basic statistics are min, max and average duration as well as the number of total completions
 */
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueryPlanStats implements QueryPlan{

    private DBConnector dbConnector;

    private Metadata metadata;


    public QueryPlanStats() {
    }

    @Autowired
    public QueryPlanStats(DBConnector dbConnector) {
        this.dbConnector=dbConnector;
    }


    public QueryResponse execute(QueryWrapper qs) {
        QueryStatsWrapper qsw = (QueryStatsWrapper)qs;
        Set<EventPair> eventPairs = qsw.getPattern().extractPairsConsecutive();
        List<Count> stats = dbConnector.getStats(qsw.getLog_name(),eventPairs);
        return new QueryResponseStats(stats);
    }

    @Override
    public void setMetadata(Metadata metadata) {
        this.metadata=metadata;
    }

}
