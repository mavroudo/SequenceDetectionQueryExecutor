package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraint;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraint;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.EventSymbol;
import com.datalab.siesta.queryprocessor.model.Patterns.ComplexPattern;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseWhyNotMatch;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.QueryPatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.model.WhyNotMatch.WhyNotMatchConfig;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;


@SpringBootTest
class QueryPlanWhyNotMatchTest {


    @Autowired
    private DBConnector dbConnector;

    @Autowired
    private QueryPatternDetection queryPatternDetection;

    private ComplexPattern getPattern() {
        EventSymbol ep1 = new EventSymbol("A", 0, "");
        EventSymbol ep2 = new EventSymbol("B", 1, "");
        EventSymbol ep3 = new EventSymbol("C", 2, "");
        List<EventSymbol> events = new ArrayList<>();
        events.add(ep1);
        events.add(ep2);
        events.add(ep3);
        return new ComplexPattern(events);
    }


    @Test
    void testWhyNotMatch(){
        QueryPatternDetectionWrapper queryPatternDetectionWrapper = new QueryPatternDetectionWrapper();
        queryPatternDetectionWrapper.setLog_name("test");
        queryPatternDetectionWrapper.setWhyNotMatchFlag(true);
        queryPatternDetectionWrapper.setWhyNotMatchConfig(new WhyNotMatchConfig(30,"minutes",2,"minutes"));

        ComplexPattern cp = this.getPattern();
        List<Constraint> constraints = new ArrayList<>(){{
            add(new TimeConstraint(0, 1, 29*60));
        }};
        cp.setConstraints(constraints);
        queryPatternDetectionWrapper.setPattern(cp);

        Metadata m = dbConnector.getMetadata(queryPatternDetectionWrapper.getLog_name());
        QueryPlan queryPlan =
                queryPatternDetection.createQueryPlan(queryPatternDetectionWrapper,m);

        QueryResponse qr = queryPlan.execute(queryPatternDetectionWrapper);
        Assertions.assertEquals(0,((QueryResponseWhyNotMatch) qr).getTrueOccurrences().size());
        Assertions.assertEquals(3,((QueryResponseWhyNotMatch) qr).getAlmostOccurrences().size());
        Assertions.assertNotNull(qr);
    }


}