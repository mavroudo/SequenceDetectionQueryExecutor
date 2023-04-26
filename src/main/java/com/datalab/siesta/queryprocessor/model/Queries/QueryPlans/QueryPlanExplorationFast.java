package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Proposition;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseExploration;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryExploreWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueryPlanExplorationFast implements QueryPlan {

    private DBConnector dbConnector;

    private Metadata metadata;


    @Autowired
    public QueryPlanExplorationFast(DBConnector dbConnector) {
        this.dbConnector = dbConnector;
    }

    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryExploreWrapper queryExploreWrapper = (QueryExploreWrapper) qw;
        Set<EventPair> pairs = queryExploreWrapper.getPattern().extractPairsConsecutive();
        List<Count> pairCount = dbConnector.getStats(queryExploreWrapper.getLog_name(), pairs);
        int lastCompletions = this.getCompletionCountOfFullFunnel(pairCount);
        List<EventPos> events = queryExploreWrapper.getPattern().getEvents();
        String lastEvent = events.get(events.size() - 1).getName();
        List<Proposition> props = this.exploreFast(lastEvent, queryExploreWrapper.getLog_name(), lastCompletions);
        props.sort(Collections.reverseOrder());
        return new QueryResponseExploration(props);
    }

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
