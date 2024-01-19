package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.Detection;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseConnector;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.ExtractedPairsForPatternDetection;
import com.datalab.siesta.queryprocessor.model.GroupOccurrences;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseBadRequestForDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseGroups;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.model.TimeStats;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import java.util.*;

/**
 * The query plan for the detection queries that have the defined the groups
 */
@Component
@RequestScope
public class QueryPlanPatternDetectionGroups extends QueryPlanPatternDetection {

    protected Map<Integer, List<EventBoth>> middleResults;

    @Autowired
    public QueryPlanPatternDetectionGroups(DBConnector dbConnector, SaseConnector saseConnector, Utils utils) {
        super(dbConnector, saseConnector, utils);
    }

    /**
     * Executes the pattern detection over groups. The execution is similar to the basic pattern detection query plan,
     * but the getMiddleResults have been override and instead of querying the IndexTable, we get the data from the
     * SingleTable and group the events based on the group of traces
     * @param qw the query pattern
     * @return the group of traces that have the query pattern and in which positions
     */
    @Override
    public QueryResponse execute(QueryWrapper qw) {
        long start = System.currentTimeMillis();
        QueryPatternDetectionWrapper qpdw = (QueryPatternDetectionWrapper) qw;
        QueryResponseBadRequestForDetection firstCheck = new QueryResponseBadRequestForDetection();
        this.getMiddleResults(qpdw, firstCheck);
        long ts_trace = System.currentTimeMillis();
        if (!firstCheck.isEmpty()) return firstCheck; //stop the process as an error was found
        QueryResponseGroups queryResponseGroups = new QueryResponseGroups();
        List<GroupOccurrences> occurrences = saseConnector.evaluateGroups(qpdw.getPattern(), middleResults);
        occurrences.forEach(x -> x.clearOccurrences(qpdw.isReturnAll()));
        long ts_eval = System.currentTimeMillis();
        queryResponseGroups.setOccurrences(occurrences);
        TimeStats timeStats = new TimeStats();
        timeStats.setTimeForPrune(ts_trace - start);
        timeStats.setTimeForValidation(ts_eval - ts_trace);
        timeStats.setTotalTime(ts_eval - start);
        queryResponseGroups.setTimeStats(timeStats);
        return queryResponseGroups;
    }

    @Override
    protected void getMiddleResults(QueryPatternDetectionWrapper qpdw, QueryResponseBadRequestForDetection qr) {
        List<ExtractedPairsForPatternDetection> multiplePairs = new ArrayList<>();
        qr = this.evaluateQuery(multiplePairs,qpdw,qr);
        if (!qr.isEmpty()) return; //There was an original error
        middleResults = dbConnector.querySingleTableGroups(qpdw.getLog_name(), qpdw.getGroupConfig()
                .getGroups(), qpdw.getPattern().getEventTypes());
    }

}
