package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.EventPair;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseStats;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryMetadataWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryStatsWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;


@Component
public class QueryPlanStats implements QueryPlan{

    private DBConnector dbConnector;


    public QueryPlanStats() {
    }

    @Autowired
    public QueryPlanStats(DBConnector dbConnector) {
        this.dbConnector=dbConnector;
    }


    public QueryResponse execute(QueryWrapper qs) {
        QueryStatsWrapper qsw = (QueryStatsWrapper)qs;
        Set<EventPair> eventPairs = qsw.getPattern().extractPairsAll();
        List<Count> stats = dbConnector.getStats(qsw.getLog_name(),eventPairs);
        return new QueryResponseStats(stats);
    }

}
