package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.EventPair;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.*;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueryPlanPatternDetection implements QueryPlan{

    @Autowired
    private DBConnector dbConnector;

    public QueryPlanPatternDetection() {
    }

    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryPatternDetectionWrapper qpdw = (QueryPatternDetectionWrapper) qw;
        Set<EventPair> pairs =qpdw.getPattern().extractPairsWithSymbols();
        List<Count> sortedPairs = this.getStats(pairs,qpdw.getLog_name());
        List<Tuple2<EventPair,Count>> combined= this.combineWithPairs(pairs,sortedPairs);
        return null;
    }


    /**
     * Query for the stats for the different pairs in the CountTable. Once get the results will sort them based
     * on their frequency and return the results.
     * @param pairs The event pairs generated
     * @param logname The name of the log file that we are searching in
     * @return The stats for the event pairs sorted by their frequency (This can be easily changed)
     */
    protected List<Count> getStats(Set<EventPair> pairs, String logname){
        List<Count> results = dbConnector.getStats(logname,pairs);
        results.sort((Count c1, Count c2) -> Integer.compare(c1.getCount(),c1.getCount())); //TODO: separate this function in order to be easily changed
        return results;
    }

    protected List<Tuple2<EventPair,Count>> combineWithPairs(Set<EventPair> pairs, List<Count> sortedCounts){
        List<Tuple2<EventPair,Count>> response = new ArrayList<>();
        for (Count c : sortedCounts){
            for(EventPair p: pairs){
                if(p.equals(c)) {
                    response.add(new Tuple2<>(p,c));
                }
            }
        }
        return response;
    }
}
