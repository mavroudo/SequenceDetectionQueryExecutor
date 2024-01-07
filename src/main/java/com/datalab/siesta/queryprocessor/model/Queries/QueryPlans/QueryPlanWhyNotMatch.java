package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseConnector;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Occurrences;
import com.datalab.siesta.queryprocessor.model.Patterns.SimplePattern;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseBadRequestForDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseBadRequestWhyNotMatch;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseWhyNotMatch;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.model.TimeStats;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.model.WhyNotMatch.AlmostMatch;
import com.datalab.siesta.queryprocessor.model.WhyNotMatch.UsingSase.WhyNotMatchSASE;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The query plan for the detection queries that have the explainability setting on
 */
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueryPlanWhyNotMatch extends QueryPlanPatternDetection {

    /**
     * A modified SASE connector that creates the uncertain stream and detects the query pattern in it.
     */
    private final WhyNotMatchSASE whyNotMatchSASE;


    @Autowired
    public QueryPlanWhyNotMatch(DBConnector dbConnector, SaseConnector saseConnector, Utils utils, WhyNotMatchSASE whyNotMatchSASE) {
        super(dbConnector, saseConnector, utils);
        this.whyNotMatchSASE = whyNotMatchSASE;
    }


    /**
     * Executes a pattern detection query that has the explainability setting on
     * @param qw the QueryPatternDetectionWrapper
     * @return the pattern detection response
     */
    @Override
    public QueryResponse execute(QueryWrapper qw) {
        long start = System.currentTimeMillis();
        QueryPatternDetectionWrapper qpdw = (QueryPatternDetectionWrapper) qw;
        QueryResponseBadRequestForDetection firstCheck = new QueryResponseBadRequestForDetection();
        //The following part is similar to the QueryPlanPatternDetection with the additional exploration of possible
        //modifications in the end
        super.getMiddleResults(qpdw, firstCheck); // finds the middle results
        if (!firstCheck.isEmpty()) return firstCheck; //stop the process as an error was found
        if (qpdw.getPattern().getItSimpler() == null) { //the pattern is not simple (we don't allow that yet)
            QueryResponseBadRequestWhyNotMatch queryResponseBadRequestWhyNotMatch = new QueryResponseBadRequestWhyNotMatch();
            queryResponseBadRequestWhyNotMatch.setSimple(false);
            return queryResponseBadRequestWhyNotMatch;
        }
        //checks if data is required from the sequence table and gets them
        if (super.requiresQueryToDB(qpdw)) { // we need to get from SeqTable
            super.retrieveTimeInformation(qpdw.getPattern(), qpdw.getLog_name(), qpdw.getFrom(), qpdw.getTill());
        }

        long ts_trace = System.currentTimeMillis();
        List<Occurrences> trueOccurrences = saseConnector.evaluate(qpdw.getPattern(), imr.getEvents(), false);
        trueOccurrences.forEach(x -> x.clearOccurrences(qpdw.isReturnAll()));
        QueryResponseWhyNotMatch queryResponseWhyNotMatch = new QueryResponseWhyNotMatch();
        queryResponseWhyNotMatch.setOccurrences(trueOccurrences);


        //Keep the traces that do not contain any occurrences
        Set<Long> foundOccurrences = trueOccurrences.stream().parallel()
                .map(Occurrences::getTraceID).collect(Collectors.toSet());
        Map<Long, List<Event>> restTraces = imr.getEvents().entrySet().stream()
                .filter(entry -> !foundOccurrences.contains(entry.getKey()))
                .map(x -> new Tuple2<>(x.getKey(), x.getValue()))
                .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));

        // Execute the whyNotMatch search
        SimplePattern sp = qpdw.getPattern().getItSimpler();
        List<AlmostMatch> almostMatches = whyNotMatchSASE.evaluate(sp,restTraces, qpdw.getUncertainty(), qpdw.getStepInSeconds(), qpdw.getK());
        // collects the time-stat information and added it to the response
        long ts_eval = System.currentTimeMillis();
        TimeStats timeStats = new TimeStats();
        timeStats.setTimeForPrune(ts_trace-start);
        timeStats.setTimeForValidation(ts_eval-ts_trace);
        timeStats.setTotalTime(ts_eval-start);
        queryResponseWhyNotMatch.setTimeStats(timeStats);
        queryResponseWhyNotMatch.setAlmostOccurrences(almostMatches);

        return queryResponseWhyNotMatch;
    }

}
