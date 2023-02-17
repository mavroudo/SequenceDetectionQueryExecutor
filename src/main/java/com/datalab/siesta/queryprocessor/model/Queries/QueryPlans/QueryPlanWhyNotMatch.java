package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseConnector;
import com.datalab.siesta.queryprocessor.model.Occurrences;
import com.datalab.siesta.queryprocessor.model.OccurrencesWhyNotMatch;
import com.datalab.siesta.queryprocessor.model.PossibleOrderOfEvents;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseBadRequestForDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseBadRequestWhyNotMatch;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseWhyNotMatch;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueryPlanWhyNotMatch extends QueryPlanPatternDetection {

    /**
     * k is equal to the error we allow for each event. In case of gap constraint this is equal to difference in the
     * position and in case of timestamp this is equal to seconds.
     */
    private int k;

    public QueryPlanWhyNotMatch(DBConnector dbConnector, SaseConnector saseConnector, Utils utils) {
        super(dbConnector, saseConnector, utils);
        k=30*60; //set to 30 minutes by default
    }


    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryPatternDetectionWrapper qpdw = (QueryPatternDetectionWrapper) qw;
        QueryResponseBadRequestForDetection firstCheck = new QueryResponseBadRequestForDetection();
        this.getMiddleResults(qpdw, firstCheck);
        if (!firstCheck.isEmpty()) return firstCheck; //stop the process as an error was found
        if (qpdw.getPattern().getItSimpler()==null){ //the pattern is not simple (we don't allow that yet)
            QueryResponseBadRequestWhyNotMatch queryResponseBadRequestWhyNotMatch = new QueryResponseBadRequestWhyNotMatch();
            queryResponseBadRequestWhyNotMatch.setSimple(false);
            return queryResponseBadRequestWhyNotMatch;
        }
        //TODO: add here to check in the Seq table if not all data available
        List<Occurrences> trueOccurrences = saseConnector.evaluate(qpdw.getPattern(), imr.getEvents(),false);
        trueOccurrences.forEach(x->x.clearOccurrences(qpdw.isReturnAll()));
        QueryResponseWhyNotMatch queryResponseWhyNotMatch = new QueryResponseWhyNotMatch();
        queryResponseWhyNotMatch.setTrueOccurrences(trueOccurrences);


        //Keep the traces that do not contain any occurrences
        Set<Long> foundOccurrences = trueOccurrences.stream().parallel()
                .map(Occurrences::getTraceID).collect(Collectors.toSet());
        Set<Long> restTraces = imr.getTrace_ids().stream().parallel()
                .filter(x-> !foundOccurrences.contains(x)).collect(Collectors.toSet());

        //Generate possible traces
        List<PossibleOrderOfEvents> possibleOrderOfEvents = imr.getEvents().entrySet().stream().parallel().filter(x-> restTraces.contains(x.getKey()))
                .map(x-> new PossibleOrderOfEvents(x.getKey(),x.getValue(), qpdw.getPattern().getConstraints(),k))
                .flatMap(x-> x.getPossibleOrderOfEvents().stream())
                .collect(Collectors.toList());

        //evaluate appearance
        List<OccurrencesWhyNotMatch> parseAppearance = saseConnector.evaluate(qpdw.getPattern(), possibleOrderOfEvents, true);

        //evaluate the constraint in the matches


        // second parsing contains both the traces that contains the patterns and the traces that lost because of the constraint
        //TODO: last step clean the second traces and create the response


        return queryResponseWhyNotMatch;
    }

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }
}
