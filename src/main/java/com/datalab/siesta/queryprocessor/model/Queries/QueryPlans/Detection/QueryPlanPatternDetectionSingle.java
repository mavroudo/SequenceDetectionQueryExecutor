package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.Detection;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseConnector;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.ExtractedPairsForPatternDetection;
import com.datalab.siesta.queryprocessor.model.Occurrences;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseBadRequestForDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponsePatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.model.TimeStats;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The query plan responsible for detecting patterns of a single event (i.e. patterns that contain 2 characters
 * and one of them is a *, +, !, ||). Since in that case IndexTable cannot be utilized, the SingleTable will be used
 * against. This procedure resembles the process of detecting patterns in groups and therefore we extend this class
 */
@Component
@RequestScope
public class QueryPlanPatternDetectionSingle extends QueryPlanPatternDetection {

    private Map<Long, List<EventBoth>> intermediateResults;

    public QueryPlanPatternDetectionSingle(DBConnector dbConnector, SaseConnector saseConnector, Utils utils) {
        super(dbConnector, saseConnector, utils);
    }

    @Override
    public QueryResponse execute(QueryWrapper qw) {
        long start = System.currentTimeMillis();
        QueryPatternDetectionWrapper qpdw = (QueryPatternDetectionWrapper) qw;
        QueryResponseBadRequestForDetection firstCheck = new QueryResponseBadRequestForDetection();
        this.getMiddleResults(qpdw, firstCheck);
        long ts_trace = System.currentTimeMillis();
        if (!firstCheck.isEmpty()) return firstCheck; //stop the process as an error was found
        QueryResponsePatternDetection queryResponsePatternDetection = new QueryResponsePatternDetection();
        List<Occurrences> occurrences = saseConnector.evaluateSmallPatterns(qpdw.getPattern(), intermediateResults);
        occurrences.forEach(x -> x.clearOccurrences(qpdw.isReturnAll()));
        long ts_eval = System.currentTimeMillis();
        queryResponsePatternDetection.setOccurrences(occurrences);
        TimeStats timeStats = new TimeStats(); //create the response stats (pruning,validation and total time)
        timeStats.setTimeForPrune(ts_trace - start);
        timeStats.setTimeForValidation(ts_eval - ts_trace);
        timeStats.setTotalTime(ts_eval - start);
        queryResponsePatternDetection.setTimeStats(timeStats); //add time-stats to the response object
        return queryResponsePatternDetection;

    }

    @Override
    protected void getMiddleResults(QueryPatternDetectionWrapper qpdw, QueryResponseBadRequestForDetection qr) {
        List<ExtractedPairsForPatternDetection> multiplePairs = new ArrayList<>();
        intermediateResults = dbConnector.getEventsFromSingleTableGroupedByTraceID(qpdw.getLog_name(),
                qpdw.getPattern().getEventTypes());

    }
}
