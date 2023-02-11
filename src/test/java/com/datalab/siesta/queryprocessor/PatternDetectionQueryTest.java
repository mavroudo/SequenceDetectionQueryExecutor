package com.datalab.siesta.queryprocessor;


import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Events.EventSymbol;
import com.datalab.siesta.queryprocessor.model.Occurrences;
import com.datalab.siesta.queryprocessor.model.Patterns.ComplexPattern;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlanPatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponsePatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.QueryPatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
        QueryResponsePatternDetection queryResponse = (QueryResponsePatternDetection) plan.execute(qpdw);
        List<Occurrences> ocs = queryResponse.getOccurrences();
        Assertions.assertEquals(3,ocs.size());
        List<Long> detectedInTraces = ocs.stream().map(Occurrences::getTraceID).collect(Collectors.toList());;
        Assertions.assertTrue(detectedInTraces.contains(1L));
        Assertions.assertTrue(detectedInTraces.contains(2L));
        Assertions.assertTrue(detectedInTraces.contains(3L));
        Assertions.assertFalse(detectedInTraces.contains(4L));
    }
    @Test
    public void simplePatternReturnAll() throws Exception{
        QueryPatternDetectionWrapper qpdw = new QueryPatternDetectionWrapper();
        ComplexPattern cp = this.getPattern();
        qpdw.setPattern(cp);
        qpdw.setLog_name("test");
        qpdw.setReturnAll(true);
        Metadata m = dbConnector.getMetadata(qpdw.getLog_name());
        Set<String> events = new HashSet<>(dbConnector.getEventNames(qpdw.getLog_name()));
        QueryPlanPatternDetection plan = (QueryPlanPatternDetection) query.createQueryPlan(qpdw,m);
        plan.setEventTypesInLog(events);
        QueryResponsePatternDetection queryResponse = (QueryResponsePatternDetection) plan.execute(qpdw);
        List<Occurrences> ocs = queryResponse.getOccurrences();
        Assertions.assertEquals(4,ocs.stream().mapToInt(x->x.getOccurrences().size()).sum());
        List<Long> detectedInTraces = ocs.stream().map(Occurrences::getTraceID).collect(Collectors.toList());;
        Assertions.assertTrue(detectedInTraces.contains(1L));
        Assertions.assertTrue(detectedInTraces.contains(2L));
        Assertions.assertTrue(detectedInTraces.contains(3L));
        Assertions.assertFalse(detectedInTraces.contains(4L));
    }
}
