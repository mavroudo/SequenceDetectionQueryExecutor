package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseConnector;
import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.ExtractedPairsForPatternDetection;
import com.datalab.siesta.queryprocessor.model.Occurrence;
import com.datalab.siesta.queryprocessor.model.Occurrences;
import com.datalab.siesta.queryprocessor.model.Patterns.SimplePattern;
import com.datalab.siesta.queryprocessor.model.Proposition;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseExploration;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryExploreWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueryPlanExplorationAccurate extends QueryPlanPatternDetection implements QueryPlan {


    @Autowired
    public QueryPlanExplorationAccurate(DBConnector dbConnector, SaseConnector saseConnector, Utils utils) {
        super(dbConnector, saseConnector, utils);
    }

    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryExploreWrapper queryExploreWrapper = (QueryExploreWrapper) qw;
        List<EventPos> events = queryExploreWrapper.getPattern().getEvents();
        String lastEvent = events.get(events.size() - 1).getName();
        List<Count> freqs = dbConnector.getCountForExploration(queryExploreWrapper.getLogname(), lastEvent);
        List<Proposition> props = new ArrayList<>();
        for (Count freq : freqs) {
            try {
                SimplePattern sp = (SimplePattern) queryExploreWrapper.getPattern().clone();
                Proposition p = this.patternDetection(sp, freq.getEventB());
                if (p != null) props.add(p);
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }

        }
        props.sort(Collections.reverseOrder());
        return new QueryResponseExploration(props);
    }

    /**
     * This will append the second event from the count to the simple pattern and the detect the exact occurrences of
     * this pattern in the dataset
     *
     * @param pattern the original pattern we want to explore future events
     * @param c       a count pair that contains the last event of the pattern and one possible extension
     * @return a proposition
     */
    protected Proposition patternDetection(SimplePattern pattern, String next) {
        List<EventPos> events = pattern.getEvents();
        events.add(new EventPos(next, events.size()));
        pattern.setEvents(events); //create the pattern
        ExtractedPairsForPatternDetection pairs = pattern.extractPairsForPatternDetection(false);
        List<Count> sortedPairs = this.getStats(pairs.getAllPairs(), metadata.getLogname());
        List<Tuple2<EventPair, Count>> combined = this.combineWithPairs(pairs.getAllPairs(), sortedPairs);
        imr = dbConnector.patterDetectionTraceIds(metadata.getLogname(), combined, metadata, pairs, null, null); //TODO:change min pairs here also
        List<Occurrences> occurrences = saseConnector.evaluate(pattern, imr.getEvents(), false);
        occurrences.forEach(x -> x.clearOccurrences(true));
        List<Occurrence> ocs = occurrences.stream().parallel().flatMap(x -> x.getOccurrences().stream()).collect(Collectors.toList());
        if (!ocs.isEmpty()) {
            double avg_duration = ocs.stream().mapToDouble(Occurrence::getDuration).sum() / ocs.size();
            return new Proposition(next, ocs.size(), avg_duration);
        } else {
            return null;
        }
    }

    @Override
    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }
}
