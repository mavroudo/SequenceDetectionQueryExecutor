package com.datalab.siesta.queryprocessor.model.Queries.QueryTypes;

import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Patterns.SimplePattern;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseExploration;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryExploreWrapper;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class QueryExplorationTest {

    @Autowired
    private DBConnector dbConnector;

    @Autowired
    private QueryExploration queryExploration;

    private SimplePattern getPattern() {
        EventPos ep1 = new EventPos("A", 0);
        EventPos ep2 = new EventPos("B", 1);
        EventPos ep3 = new EventPos("C", 2);
        List<EventPos> events = new ArrayList<>();
        events.add(ep1);
        events.add(ep2);
        events.add(ep3);
        return new SimplePattern(events);
    }


    @Test
    void testExplorationFast() {
        QueryExploreWrapper queryExploreWrapper = new QueryExploreWrapper();
        queryExploreWrapper.setLog_name("test");
        queryExploreWrapper.setPattern(this.getPattern());
        Metadata m = dbConnector.getMetadata(queryExploreWrapper.getLog_name());
        QueryPlan queryPlan = queryExploration.createQueryPlan(queryExploreWrapper,m);
        QueryResponseExploration queryResponse = (QueryResponseExploration) queryPlan.execute(queryExploreWrapper);
        assertEquals(4, queryResponse.getPropositions().size());
    }

    @Test
    void testExplorationAccurate() {
        QueryExploreWrapper queryExploreWrapper = new QueryExploreWrapper();
        queryExploreWrapper.setLog_name("test");
        queryExploreWrapper.setPattern(this.getPattern());
        queryExploreWrapper.setMode("accurate");
        Metadata m = dbConnector.getMetadata(queryExploreWrapper.getLog_name());
        QueryPlan queryPlan = queryExploration.createQueryPlan(queryExploreWrapper,m);
        QueryResponseExploration queryResponse = (QueryResponseExploration) queryPlan.execute(queryExploreWrapper);
        assertEquals(3, queryResponse.getPropositions().size());
    }

    @Test
    void testExplorationHybrid() {
        QueryExploreWrapper queryExploreWrapper = new QueryExploreWrapper();
        queryExploreWrapper.setLog_name("test");
        queryExploreWrapper.setPattern(this.getPattern());
        queryExploreWrapper.setMode("hybrid");
        queryExploreWrapper.setK(3);
        Metadata m = dbConnector.getMetadata(queryExploreWrapper.getLog_name());
        QueryPlan queryPlan = queryExploration.createQueryPlan(queryExploreWrapper,m);
        QueryResponseExploration queryResponse = (QueryResponseExploration) queryPlan.execute(queryExploreWrapper);
        assertEquals(2, queryResponse.getPropositions().size());
    }
}