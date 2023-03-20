package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseConnector;
import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
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
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueryPlanPatternDetectionGroups extends QueryPlanPatternDetection {

    private Map<Integer, List<EventBoth>> middleResults;

    @Autowired
    public QueryPlanPatternDetectionGroups(DBConnector dbConnector, SaseConnector saseConnector, Utils utils) {
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
        boolean fromOrTillSet = qpdw.getFrom() != null || qpdw.getTill() != null;
        Tuple2<Integer, Set<EventPair>> pairs = qpdw.getPattern().extractPairsForPatternDetection(fromOrTillSet);
        List<Count> sortedPairs = this.getStats(pairs._2, qpdw.getLog_name());
        List<Tuple2<EventPair, Count>> combined = this.combineWithPairs(pairs._2, sortedPairs);
        this.firstParsing(qpdw, pairs._2, combined,qr); // checks if all are correctly set before start querying
        if (!qr.isEmpty()) {//There was an original error
            return;
        }
        middleResults = dbConnector.querySingleTableGroups(qpdw.getLog_name(), qpdw.getGroupConfig()
                .getGroups(), qpdw.getPattern().getEventTypes());
    }
}
