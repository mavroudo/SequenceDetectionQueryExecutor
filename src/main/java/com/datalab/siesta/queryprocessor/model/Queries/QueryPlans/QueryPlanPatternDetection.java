package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans;

import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraint;
import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexMiddleResult;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseBadRequestForDetection;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueryPlanPatternDetection implements QueryPlan {

    @Autowired
    private DBConnector dbConnector;

    private Metadata metadata;

    private int minPairs;

    private IndexMiddleResult imr;

    public IndexMiddleResult getImr() { //no need for this is just for testing
        return imr;
    }

    public QueryPlanPatternDetection() {
        minPairs = -1;
    }

    public void setMinPairs(int minPairs) {
        this.minPairs = minPairs;
    }

    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryPatternDetectionWrapper qpdw = (QueryPatternDetectionWrapper) qw;
        Set<EventPair> pairs = qpdw.getPattern().extractPairsWithSymbols();
        List<Count> sortedPairs = this.getStats(pairs, qpdw.getLog_name());
        List<Tuple2<EventPair, Count>> combined = this.combineWithPairs(pairs, sortedPairs);
        QueryResponse qr = this.firstParsing(pairs, combined);
        if (qr != null) return qr; //There was an original error
        minPairs = minPairs == -1? combined.size() : minPairs; //TODO: modify it to pass the arguments during initialization
        imr = dbConnector.patterDetectionTraceIds(qpdw.getLog_name(), combined, metadata, minPairs);
        return null;
    }



    @Override
    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }


    /**
     * Query for the stats for the different pairs in the CountTable. Once get the results will sort them based
     * on their frequency and return the results.
     *
     * @param pairs   The event pairs generated
     * @param logname The name of the log file that we are searching in
     * @return The stats for the event pairs sorted by their frequency (This can be easily changed)
     */
    protected List<Count> getStats(Set<EventPair> pairs, String logname) {
        List<Count> results = dbConnector.getStats(logname, pairs);
        results.sort((Count c1, Count c2) -> Integer.compare(c1.getCount(), c1.getCount())); //TODO: separate this function in order to be easily changed
        return results;
    }

    protected List<Tuple2<EventPair, Count>> combineWithPairs(Set<EventPair> pairs, List<Count> sortedCounts) {
        List<Tuple2<EventPair, Count>> response = new ArrayList<>();
        for (Count c : sortedCounts) {
            for (EventPair p : pairs) {
                if (p.equals(c)) {
                    response.add(new Tuple2<>(p, c));
                }
            }
        }
        return response;
    }

    protected QueryResponseBadRequestForDetection firstParsing(Set<EventPair> pairs, List<Tuple2<EventPair, Count>> combined) {
        List<EventPair> inPairs = new ArrayList<>();
        if (pairs.size() != combined.size()) { //find non-existing event pairs
            for (Tuple2<EventPair, Count> c : combined) {
                if (!pairs.contains(c._1)) {
                    inPairs.add(c._1);
                }
            }
            if (inPairs.size() > 0) {
                QueryResponseBadRequestForDetection qr = new QueryResponseBadRequestForDetection();
                qr.setNonExistingPairs(inPairs);
                return qr;
            }
        }
        for (Tuple2<EventPair, Count> c : combined) { //Find constraints that do not hold in all db
            if (c._1.getConstraint() instanceof TimeConstraint) {
                TimeConstraint tc = (TimeConstraint) c._1.getConstraint();
                if (!tc.isConstraintHolds(c._2)) {
                    inPairs.add(c._1);
                }
            }
        }
        if (inPairs.size() > 0) {
            QueryResponseBadRequestForDetection qr = new QueryResponseBadRequestForDetection();
            qr.setConstraintsNotFulfilled(inPairs);
            return qr;
        } else return null;
    }

}
