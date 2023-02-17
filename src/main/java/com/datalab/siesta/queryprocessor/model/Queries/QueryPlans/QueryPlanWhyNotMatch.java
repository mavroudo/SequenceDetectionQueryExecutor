package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseConnector;
import com.datalab.siesta.queryprocessor.model.WhyNotMatchResponse;
import com.datalab.siesta.queryprocessor.model.PossiblePattern;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseBadRequestForDetection;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.List;
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
    }


    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryPatternDetectionWrapper qpdw = (QueryPatternDetectionWrapper) qw;
        QueryResponseBadRequestForDetection firstCheck = new QueryResponseBadRequestForDetection();
        this.getMiddleResults(qpdw, firstCheck);
        if (!firstCheck.isEmpty()) return firstCheck; //stop the process as an error was found
        //TODO: add check if the complexPattern is simple and if not return a QueryResponseBadWhyNotMatch
        //TODO: add here to check in the Single table if not all data available
        //At this point the imp has been set
        List<PossiblePattern> possiblePatterns = imr.getEvents().entrySet().stream().parallel().map(e ->
                        new PossiblePattern(e.getKey(), e.getValue(), qpdw.getPattern().getConstraints(), k))
                .flatMap(x->x.getPossiblePatterns().stream()) //get all the changes also
                .collect(Collectors.toList());
        WhyNotMatchResponse firstParsing = saseConnector.evaluate(qpdw.getPattern(),possiblePatterns,true);

        //these patterns contain the pattern but no constraints have been verified
        List<PossiblePattern> possiblePatterns2 = firstParsing.getFound();
        WhyNotMatchResponse secondParsing = saseConnector.evaluate(qpdw.getPattern(),possiblePatterns2,false);

        // second parsing contains both the traces that contains the patterns and the traces that lost because of the constraint
        //TODO: last step clean the second traces and create the response


        return new QueryResponseBadRequestForDetection(); //TODO: change that also
    }

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }
}
