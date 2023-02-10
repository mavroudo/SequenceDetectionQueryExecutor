package com.datalab.siesta.queryprocessor;


import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Events.EventSymbol;
import com.datalab.siesta.queryprocessor.model.Patterns.ComplexPattern;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlanPatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.QueryPatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@SpringBootTest
public class PatternDetectionQueryTest {

    @Autowired
    private QueryPatternDetection query;

    @Autowired
    private DBConnector dbConnector;

    private ComplexPattern getPattern(){
        EventSymbol ep1 = new EventSymbol("A",0,"");
        EventSymbol ep2 = new EventSymbol("B",1,"");
        EventSymbol ep3 = new EventSymbol("C",2,"");
        List<EventSymbol> events = new ArrayList<>();
        events.add(ep1);
        events.add(ep2);
        events.add(ep3);
        return new ComplexPattern(events);
    }


    @Test
    public void simplePattern() throws Exception{
        QueryPatternDetectionWrapper qpdw = new QueryPatternDetectionWrapper();
        ComplexPattern cp = this.getPattern();
        qpdw.setPattern(cp);
        qpdw.setLog_name("test");
        Metadata m = dbConnector.getMetadata(qpdw.getLog_name());
        Set<String> events = new HashSet<>(dbConnector.getEventNames(qpdw.getLog_name()));
        QueryPlanPatternDetection plan = (QueryPlanPatternDetection) query.createQueryPlan(qpdw,m);
        plan.setEventTypesInLog(events);
        QueryResponse queryResponse = plan.execute(qpdw);
        System.out.println("hey");
    }
}
