package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans;

import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.EventSymbol;
import com.datalab.siesta.queryprocessor.model.Patterns.ComplexPattern;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.QueryPatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

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


    public void testWhyNotMatch(){
        QueryPatternDetectionWrapper queryPatternDetectionWrapper = new QueryPatternDetectionWrapper();
        queryPatternDetectionWrapper.setLog_name("test");
        queryPatternDetectionWrapper.setWhyNotMatchFlag(true);


        Metadata m = dbConnector.getMetadata(queryPatternDetectionWrapper.getLog_name());
        QueryPlanWhyNotMatch queryPlanWhyNotMatch = (QueryPlanWhyNotMatch)
                queryPatternDetection.createQueryPlan(queryPatternDetectionWrapper,m);



    }


}