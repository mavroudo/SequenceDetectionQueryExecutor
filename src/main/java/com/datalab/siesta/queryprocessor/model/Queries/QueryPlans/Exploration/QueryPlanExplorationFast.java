package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.Exploration;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Proposition;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseExploration;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryExploreWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * The query plan for the fast detection of continuation for the query pattern
 */
@Component
@RequestScope
public class QueryPlanExplorationFast implements QueryPlan {

    private DBConnector dbConnector;

    private Metadata metadata;


    @Autowired
    public QueryPlanExplorationFast(DBConnector dbConnector) {
        this.dbConnector = dbConnector;
    }

    /**
     * Using the CountTable, the next possible events are retrieved and sorted from the most frequent next event, to the
     * least frequent. Then these propositions are added to a QueryResponseExploration object and returned.
     * @param qw the QueryPatternDetectionWrapper
     * @return the possible next events sorted based on frequency, in the form of propositions
     */
    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryExploreWrapper queryExploreWrapper = (QueryExploreWrapper) qw;
        Set<EventPair> pairs = queryExploreWrapper.getPattern().extractPairsConsecutive();
        //approximate the total completions of the pattern based on the occurrences of the consecutive pairs
        List<Count> pairCount = dbConnector.getStats(queryExploreWrapper.getLog_name(), pairs);
        int lastCompletions = this.getCompletionCountOfFullFunnel(pairCount);
        List<EventPos> events = queryExploreWrapper.getPattern().getEvents();
        String lastEvent = events.get(events.size() - 1).getName();
        List<Proposition> props = this.exploreFast(lastEvent, queryExploreWrapper.getLog_name(), lastCompletions);
        props.sort(Collections.reverseOrder());
        return new QueryResponseExploration(props);
    }

    /**
     * This function approximates the total occurrences of the query pattern if another event is appended at the end.
     * The list of the last events is retrieved from the CountTable. The approximation is calculated as the min of the
     * total completions of the query pattern and the number of occurences of the last event of the query pattern and the
     * possible next event
     * @param lastEvent the last event of the query pattern
     * @param logname the database log
     * @param lastCompletions the approximation of the total completions of the query pattern, based on stats
     * @return a list of propositions for the continuation of the query pattern sorted based on the approximate completion
     * times
     */
    protected List<Proposition> exploreFast(String lastEvent, String logname, int lastCompletions) {
        List<Proposition> propositions = new ArrayList<>();
        List<Count> freqs = dbConnector.getCountForExploration(logname, lastEvent);
        for (Count freq : freqs) {
            int upper = Math.min(lastCompletions, freq.getCount());
            Proposition prop = new Proposition(freq.getEventB(), upper,
                    (double) freq.getSum_duration() / freq.getCount());
            if (upper != 0)
                propositions.add(prop);
        }
        return propositions;
    }

    /**
     * Approximate the total completions of the query pattern based on the number of occurrences of the consecutive
     * pairs
     * @param counts stats for each consecutive pair
     * @return an approximation of the total completions of the query pattern
     */
    protected int getCompletionCountOfFullFunnel(List<Count> counts) {
        int partialCount = Integer.MAX_VALUE;
        for (Count c : counts) {
            if (c.getCount() < partialCount) {
                partialCount = c.getCount();
            }
        }
        return partialCount;
    }


    @Override
    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }
}
