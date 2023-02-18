package com.datalab.siesta.queryprocessor;


import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraint;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraint;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Events.EventSymbol;
import com.datalab.siesta.queryprocessor.model.Occurrences;
import com.datalab.siesta.queryprocessor.model.Patterns.ComplexPattern;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlanPatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponsePatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.QueryPatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.Timestamp;
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
    public void simplePatternTS() throws Exception {
        QueryPatternDetectionWrapper qpdw = new QueryPatternDetectionWrapper();
        ComplexPattern cp = this.getPattern();
        qpdw.setPattern(cp);
        qpdw.setLog_name("test");
        Metadata m = dbConnector.getMetadata(qpdw.getLog_name());
        Assertions.assertEquals("timestamps", m.getMode());
        Set<String> events = new HashSet<>(dbConnector.getEventNames(qpdw.getLog_name()));
        QueryPlanPatternDetection plan = (QueryPlanPatternDetection) query.createQueryPlan(qpdw, m);
        plan.setEventTypesInLog(events);
        QueryResponsePatternDetection queryResponse = (QueryResponsePatternDetection) plan.execute(qpdw);
        List<Occurrences> ocs = queryResponse.getOccurrences();
        Assertions.assertEquals(3, ocs.size());
        List<Long> detectedInTraces = ocs.stream().map(Occurrences::getTraceID).collect(Collectors.toList());
        Assertions.assertTrue(detectedInTraces.contains(1L));
        Assertions.assertTrue(detectedInTraces.contains(2L));
        Assertions.assertTrue(detectedInTraces.contains(3L));
        Assertions.assertFalse(detectedInTraces.contains(4L));
    }


    @Test
    public void simplePatternReturnAllTS() throws Exception {
        QueryPatternDetectionWrapper qpdw = new QueryPatternDetectionWrapper();
        ComplexPattern cp = this.getPattern();
        qpdw.setPattern(cp);
        qpdw.setLog_name("test");
        qpdw.setReturnAll(true);
        Metadata m = dbConnector.getMetadata(qpdw.getLog_name());
        Assertions.assertEquals("timestamps", m.getMode());
        Set<String> events = new HashSet<>(dbConnector.getEventNames(qpdw.getLog_name()));
        QueryPlanPatternDetection plan = (QueryPlanPatternDetection) query.createQueryPlan(qpdw, m);
        plan.setEventTypesInLog(events);
        QueryResponsePatternDetection queryResponse = (QueryResponsePatternDetection) plan.execute(qpdw);
        List<Occurrences> ocs = queryResponse.getOccurrences();
        Assertions.assertEquals(4, ocs.stream().mapToInt(x -> x.getOccurrences().size()).sum());
        List<Long> detectedInTraces = ocs.stream().map(Occurrences::getTraceID).collect(Collectors.toList());
        Assertions.assertTrue(detectedInTraces.contains(1L));
        Assertions.assertTrue(detectedInTraces.contains(2L));
        Assertions.assertTrue(detectedInTraces.contains(3L));
        Assertions.assertFalse(detectedInTraces.contains(4L));
    }

    @Test
    public void simplePatternPos() throws Exception {
        QueryPatternDetectionWrapper qpdw = new QueryPatternDetectionWrapper();
        ComplexPattern cp = this.getPattern();
        qpdw.setPattern(cp);
        qpdw.setLog_name("test_pos");
        Metadata m = dbConnector.getMetadata(qpdw.getLog_name());
        Assertions.assertEquals("positions", m.getMode());
        Set<String> events = new HashSet<>(dbConnector.getEventNames(qpdw.getLog_name()));
        QueryPlanPatternDetection plan = (QueryPlanPatternDetection) query.createQueryPlan(qpdw, m);
        plan.setEventTypesInLog(events);
        QueryResponsePatternDetection queryResponse = (QueryResponsePatternDetection) plan.execute(qpdw);
        List<Occurrences> ocs = queryResponse.getOccurrences();
        Assertions.assertEquals(3, ocs.size());
        List<Long> detectedInTraces = ocs.stream().map(Occurrences::getTraceID).collect(Collectors.toList());
        Assertions.assertTrue(detectedInTraces.contains(1L));
        Assertions.assertTrue(detectedInTraces.contains(2L));
        Assertions.assertTrue(detectedInTraces.contains(3L));
        Assertions.assertFalse(detectedInTraces.contains(4L));
    }

    @Test
    public void simplePatternReturnAllPos() throws Exception {
        QueryPatternDetectionWrapper qpdw = new QueryPatternDetectionWrapper();
        ComplexPattern cp = this.getPattern();
        qpdw.setPattern(cp);
        qpdw.setLog_name("test_pos");
        qpdw.setReturnAll(true);
        Metadata m = dbConnector.getMetadata(qpdw.getLog_name());
        Assertions.assertEquals("positions", m.getMode());
        Set<String> events = new HashSet<>(dbConnector.getEventNames(qpdw.getLog_name()));
        QueryPlanPatternDetection plan = (QueryPlanPatternDetection) query.createQueryPlan(qpdw, m);
        plan.setEventTypesInLog(events);
        QueryResponsePatternDetection queryResponse = (QueryResponsePatternDetection) plan.execute(qpdw);
        List<Occurrences> ocs = queryResponse.getOccurrences();
        Assertions.assertEquals(4, ocs.stream().mapToInt(x -> x.getOccurrences().size()).sum());
        List<Long> detectedInTraces = ocs.stream().map(Occurrences::getTraceID).collect(Collectors.toList());
        Assertions.assertTrue(detectedInTraces.contains(1L));
        Assertions.assertTrue(detectedInTraces.contains(2L));
        Assertions.assertTrue(detectedInTraces.contains(3L));
        Assertions.assertFalse(detectedInTraces.contains(4L));
    }

    @Test
    public void timeConstraintWithin() throws Exception {
        QueryPatternDetectionWrapper qpdw = new QueryPatternDetectionWrapper();
        List<Constraint> constraints = new ArrayList<>() {{
//            add(new TimeConstraint(0, 1, 30 * 60));
            add(new TimeConstraint(0, 1, 30, "minutes"));
        }};
        ComplexPattern cp = this.getPattern();
        cp.setConstraints(constraints);
        Assertions.assertEquals(5, cp.extractPairsForPatternDetection().size());
        qpdw.setPattern(cp);
        qpdw.setLog_name("test");
        Metadata m = dbConnector.getMetadata(qpdw.getLog_name());
        Assertions.assertEquals("timestamps", m.getMode());
        Set<String> events = new HashSet<>(dbConnector.getEventNames(qpdw.getLog_name()));
        QueryPlanPatternDetection plan = (QueryPlanPatternDetection) query.createQueryPlan(qpdw, m);
        plan.setEventTypesInLog(events);
        plan.setMinPairs(3); //remove the double pairs created by the constraints from the necessary pairs
        QueryResponsePatternDetection queryResponse = (QueryResponsePatternDetection) plan.execute(qpdw);
        List<Occurrences> ocs = queryResponse.getOccurrences();
        Assertions.assertEquals(3, ocs.size());
        List<Long> detectedInTraces = ocs.stream().map(Occurrences::getTraceID).collect(Collectors.toList());

        Assertions.assertTrue(detectedInTraces.contains(1L));
        Assertions.assertTrue(detectedInTraces.contains(2L));
        Assertions.assertTrue(detectedInTraces.contains(3L));
        Assertions.assertFalse(detectedInTraces.contains(4L));

        Occurrences trace1 = ocs.stream().filter(x -> x.getTraceID() == 1).collect(Collectors.toList()).get(0);
        List<EventBoth> eventsMatch = trace1.getOccurrences().get(0).getOccurrence();
        Assertions.assertEquals(Timestamp.valueOf("2020-08-15 14:56:42"), eventsMatch.get(0).getTimestamp());
        Assertions.assertEquals(Timestamp.valueOf("2020-08-15 15:26:42"), eventsMatch.get(1).getTimestamp());
        Assertions.assertEquals(Timestamp.valueOf("2020-08-15 17:26:42"), eventsMatch.get(2).getTimestamp());

        trace1 = ocs.stream().filter(x -> x.getTraceID() == 2).collect(Collectors.toList()).get(0);
        eventsMatch.clear();
        eventsMatch = trace1.getOccurrences().get(0).getOccurrence();
        Assertions.assertEquals(Timestamp.valueOf("2020-08-15 14:56:42"), eventsMatch.get(0).getTimestamp());
        Assertions.assertEquals(Timestamp.valueOf("2020-08-15 15:26:42"), eventsMatch.get(1).getTimestamp());
        Assertions.assertEquals(Timestamp.valueOf("2020-08-15 19:26:42"), eventsMatch.get(2).getTimestamp());

        trace1 = ocs.stream().filter(x -> x.getTraceID() == 3).collect(Collectors.toList()).get(0);
        eventsMatch.clear();
        eventsMatch = trace1.getOccurrences().get(0).getOccurrence();
        Assertions.assertEquals(Timestamp.valueOf("2020-08-15 12:56:42"), eventsMatch.get(0).getTimestamp());
        Assertions.assertEquals(Timestamp.valueOf("2020-08-15 13:26:42"), eventsMatch.get(1).getTimestamp());
        Assertions.assertEquals(Timestamp.valueOf("2020-08-15 18:26:42"), eventsMatch.get(2).getTimestamp());
    }

    @Test
    public void timeConstraintWithinFromSeq() throws Exception {
        QueryPatternDetectionWrapper qpdw = new QueryPatternDetectionWrapper();
        List<Constraint> constraints = new ArrayList<>() {{
            add(new TimeConstraint(0, 1, 30 * 60));
        }};
        ComplexPattern cp = this.getPattern();
        cp.setConstraints(constraints);
        Assertions.assertEquals(5, cp.extractPairsForPatternDetection().size());
        qpdw.setPattern(cp);
        qpdw.setLog_name("test_pos");
        Metadata m = dbConnector.getMetadata(qpdw.getLog_name());
        Assertions.assertEquals("positions", m.getMode());
        Set<String> events = new HashSet<>(dbConnector.getEventNames(qpdw.getLog_name()));
        QueryPlanPatternDetection plan = (QueryPlanPatternDetection) query.createQueryPlan(qpdw, m);
        plan.setEventTypesInLog(events);
        plan.setMinPairs(3); //remove the double pairs created by the constraints from the necessary pairs
        QueryResponsePatternDetection queryResponse = (QueryResponsePatternDetection) plan.execute(qpdw);
        List<Occurrences> ocs = queryResponse.getOccurrences();
        Assertions.assertEquals(3, ocs.size());
        List<Long> detectedInTraces = ocs.stream().map(Occurrences::getTraceID).collect(Collectors.toList());

        Assertions.assertTrue(detectedInTraces.contains(1L));
        Assertions.assertTrue(detectedInTraces.contains(2L));
        Assertions.assertTrue(detectedInTraces.contains(3L));
        Assertions.assertFalse(detectedInTraces.contains(4L));

        Occurrences trace1 = ocs.stream().filter(x -> x.getTraceID() == 1).collect(Collectors.toList()).get(0);
        List<EventBoth> eventsMatch = trace1.getOccurrences().get(0).getOccurrence();
        Assertions.assertEquals(Timestamp.valueOf("2020-08-15 14:56:42"), eventsMatch.get(0).getTimestamp());
        Assertions.assertEquals(Timestamp.valueOf("2020-08-15 15:26:42"), eventsMatch.get(1).getTimestamp());
        Assertions.assertEquals(Timestamp.valueOf("2020-08-15 17:26:42"), eventsMatch.get(2).getTimestamp());

        trace1 = ocs.stream().filter(x -> x.getTraceID() == 2).collect(Collectors.toList()).get(0);
        eventsMatch.clear();
        eventsMatch = trace1.getOccurrences().get(0).getOccurrence();
        Assertions.assertEquals(Timestamp.valueOf("2020-08-15 14:56:42"), eventsMatch.get(0).getTimestamp());
        Assertions.assertEquals(Timestamp.valueOf("2020-08-15 15:26:42"), eventsMatch.get(1).getTimestamp());
        Assertions.assertEquals(Timestamp.valueOf("2020-08-15 19:26:42"), eventsMatch.get(2).getTimestamp());

        trace1 = ocs.stream().filter(x -> x.getTraceID() == 3).collect(Collectors.toList()).get(0);
        eventsMatch.clear();
        eventsMatch = trace1.getOccurrences().get(0).getOccurrence();
        Assertions.assertEquals(Timestamp.valueOf("2020-08-15 12:56:42"), eventsMatch.get(0).getTimestamp());
        Assertions.assertEquals(Timestamp.valueOf("2020-08-15 13:26:42"), eventsMatch.get(1).getTimestamp());
        Assertions.assertEquals(Timestamp.valueOf("2020-08-15 18:26:42"), eventsMatch.get(2).getTimestamp());
    }

    @Test
    public void timeConstraintAtleast() throws Exception {
        QueryPatternDetectionWrapper qpdw = new QueryPatternDetectionWrapper();
        List<Constraint> constraints = new ArrayList<>() {{
            TimeConstraint t = new TimeConstraint(0, 1, 8 * 60 * 60);
            t.setMethod("atleast");
            add(t);
        }};
        ComplexPattern cp = this.getPattern();
        cp.setConstraints(constraints);
        Assertions.assertEquals(5, cp.extractPairsForPatternDetection().size());
        qpdw.setPattern(cp);
        qpdw.setLog_name("test");
        Metadata m = dbConnector.getMetadata(qpdw.getLog_name());
        Assertions.assertEquals("timestamps", m.getMode());
        Set<String> events = new HashSet<>(dbConnector.getEventNames(qpdw.getLog_name()));
        QueryPlanPatternDetection plan = (QueryPlanPatternDetection) query.createQueryPlan(qpdw, m);
        plan.setEventTypesInLog(events);
        plan.setMinPairs(3); //remove the double pairs created by the constraints from the necessary pairs
        QueryResponsePatternDetection queryResponse = (QueryResponsePatternDetection) plan.execute(qpdw);
        List<Occurrences> ocs = queryResponse.getOccurrences();
        Assertions.assertEquals(1, ocs.size());
        List<Long> detectedInTraces = ocs.stream().map(Occurrences::getTraceID).collect(Collectors.toList());

        Assertions.assertFalse(detectedInTraces.contains(1L));
        Assertions.assertFalse(detectedInTraces.contains(2L));
        Assertions.assertTrue(detectedInTraces.contains(3L));
        Assertions.assertFalse(detectedInTraces.contains(4L));

        Occurrences trace1 = ocs.stream().filter(x -> x.getTraceID() == 3).collect(Collectors.toList()).get(0);
        List<EventBoth> eventsMatch = trace1.getOccurrences().get(0).getOccurrence();
        Assertions.assertEquals(Timestamp.valueOf("2020-08-15 07:56:42"), eventsMatch.get(0).getTimestamp());
        Assertions.assertEquals(Timestamp.valueOf("2020-08-15 19:26:42"), eventsMatch.get(1).getTimestamp());
        Assertions.assertEquals(Timestamp.valueOf("2020-08-15 20:26:42"), eventsMatch.get(2).getTimestamp());
    }

    @Test
    public void gapConstraintWithin() throws Exception {
        QueryPatternDetectionWrapper qpdw = new QueryPatternDetectionWrapper();
        List<Constraint> constraints = new ArrayList<>() {{
            add(new GapConstraint(0, 1, 1));
        }};
        ComplexPattern cp = this.getPattern();
        cp.setConstraints(constraints);
        Assertions.assertEquals(5, cp.extractPairsForPatternDetection().size());
        qpdw.setPattern(cp);
        qpdw.setLog_name("test_pos");
        Metadata m = dbConnector.getMetadata(qpdw.getLog_name());
        Assertions.assertEquals("positions", m.getMode());
        Set<String> events = new HashSet<>(dbConnector.getEventNames(qpdw.getLog_name()));
        QueryPlanPatternDetection plan = (QueryPlanPatternDetection) query.createQueryPlan(qpdw, m);
        plan.setEventTypesInLog(events);
        plan.setMinPairs(3); //remove the double pairs created by the constraints from the necessary pairs
        QueryResponsePatternDetection queryResponse = (QueryResponsePatternDetection) plan.execute(qpdw);
        List<Occurrences> ocs = queryResponse.getOccurrences();
        Assertions.assertEquals(3, ocs.size());
        List<Long> detectedInTraces = ocs.stream().map(Occurrences::getTraceID).collect(Collectors.toList());

        Assertions.assertTrue(detectedInTraces.contains(1L));
        Assertions.assertTrue(detectedInTraces.contains(2L));
        Assertions.assertTrue(detectedInTraces.contains(3L));
        Assertions.assertFalse(detectedInTraces.contains(4L));

        Occurrences trace1 = ocs.stream().filter(x -> x.getTraceID() == 1).collect(Collectors.toList()).get(0);
        List<EventBoth> eventsMatch = trace1.getOccurrences().get(0).getOccurrence();
        Assertions.assertEquals(1, eventsMatch.get(0).getPosition());
        Assertions.assertEquals(2, eventsMatch.get(1).getPosition());
        Assertions.assertEquals(3, eventsMatch.get(2).getPosition());

        trace1 = ocs.stream().filter(x -> x.getTraceID() == 2).collect(Collectors.toList()).get(0);
        eventsMatch.clear();
        eventsMatch = trace1.getOccurrences().get(0).getOccurrence();
        Assertions.assertEquals(0, eventsMatch.get(0).getPosition());
        Assertions.assertEquals(1, eventsMatch.get(1).getPosition());
        Assertions.assertEquals(4, eventsMatch.get(2).getPosition());

        trace1 = ocs.stream().filter(x -> x.getTraceID() == 3).collect(Collectors.toList()).get(0);
        eventsMatch.clear();
        eventsMatch = trace1.getOccurrences().get(0).getOccurrence();
        Assertions.assertEquals(2, eventsMatch.get(0).getPosition());
        Assertions.assertEquals(3, eventsMatch.get(1).getPosition());
        Assertions.assertEquals(4, eventsMatch.get(2).getPosition());
    }

    @Test
    public void gapConstraintWithinFromSeq() throws Exception {
        QueryPatternDetectionWrapper qpdw = new QueryPatternDetectionWrapper();
        List<Constraint> constraints = new ArrayList<>() {{
            add(new GapConstraint(0, 1, 1));
        }};
        ComplexPattern cp = this.getPattern();
        cp.setConstraints(constraints);
        Assertions.assertEquals(5, cp.extractPairsForPatternDetection().size());
        qpdw.setPattern(cp);
        qpdw.setLog_name("test");
        Metadata m = dbConnector.getMetadata(qpdw.getLog_name());
        Assertions.assertEquals("timestamps", m.getMode());
        Set<String> events = new HashSet<>(dbConnector.getEventNames(qpdw.getLog_name()));
        QueryPlanPatternDetection plan = (QueryPlanPatternDetection) query.createQueryPlan(qpdw, m);
        plan.setEventTypesInLog(events);
        plan.setMinPairs(3); //remove the double pairs created by the constraints from the necessary pairs
        QueryResponsePatternDetection queryResponse = (QueryResponsePatternDetection) plan.execute(qpdw);
        List<Occurrences> ocs = queryResponse.getOccurrences();
        Assertions.assertEquals(3, ocs.size());
        List<Long> detectedInTraces = ocs.stream().map(Occurrences::getTraceID).collect(Collectors.toList());

        Assertions.assertTrue(detectedInTraces.contains(1L));
        Assertions.assertTrue(detectedInTraces.contains(2L));
        Assertions.assertTrue(detectedInTraces.contains(3L));
        Assertions.assertFalse(detectedInTraces.contains(4L));

        Occurrences trace1 = ocs.stream().filter(x -> x.getTraceID() == 1).collect(Collectors.toList()).get(0);
        List<EventBoth> eventsMatch = trace1.getOccurrences().get(0).getOccurrence();
        Assertions.assertEquals(1, eventsMatch.get(0).getPosition());
        Assertions.assertEquals(2, eventsMatch.get(1).getPosition());
        Assertions.assertEquals(3, eventsMatch.get(2).getPosition());

        trace1 = ocs.stream().filter(x -> x.getTraceID() == 2).collect(Collectors.toList()).get(0);
        eventsMatch.clear();
        eventsMatch = trace1.getOccurrences().get(0).getOccurrence();
        Assertions.assertEquals(0, eventsMatch.get(0).getPosition());
        Assertions.assertEquals(1, eventsMatch.get(1).getPosition());
        Assertions.assertEquals(4, eventsMatch.get(2).getPosition());

        trace1 = ocs.stream().filter(x -> x.getTraceID() == 3).collect(Collectors.toList()).get(0);
        eventsMatch.clear();
        eventsMatch = trace1.getOccurrences().get(0).getOccurrence();
        Assertions.assertEquals(2, eventsMatch.get(0).getPosition());
        Assertions.assertEquals(3, eventsMatch.get(1).getPosition());
        Assertions.assertEquals(4, eventsMatch.get(2).getPosition());
    }

    @Test
    public void gapConstraintAtleast() throws Exception {
        QueryPatternDetectionWrapper qpdw = new QueryPatternDetectionWrapper();
        List<Constraint> constraints = new ArrayList<>() {{
            GapConstraint t = new GapConstraint(0, 1, 5);
            t.setMethod("atleast");
            add(t);
        }};
        ComplexPattern cp = this.getPattern();
        cp.setConstraints(constraints);
        Assertions.assertEquals(5, cp.extractPairsForPatternDetection().size());
        qpdw.setPattern(cp);
        qpdw.setLog_name("test_pos");
        Metadata m = dbConnector.getMetadata(qpdw.getLog_name());
        Assertions.assertEquals("positions", m.getMode());
        Set<String> events = new HashSet<>(dbConnector.getEventNames(qpdw.getLog_name()));
        QueryPlanPatternDetection plan = (QueryPlanPatternDetection) query.createQueryPlan(qpdw, m);
        plan.setEventTypesInLog(events);
        plan.setMinPairs(3); //remove the double pairs created by the constraints from the necessary pairs
        QueryResponsePatternDetection queryResponse = (QueryResponsePatternDetection) plan.execute(qpdw);
        List<Occurrences> ocs = queryResponse.getOccurrences();
        Assertions.assertEquals(2, ocs.size());
        List<Long> detectedInTraces = ocs.stream().map(Occurrences::getTraceID).collect(Collectors.toList());

        Assertions.assertFalse(detectedInTraces.contains(1L));
        Assertions.assertTrue(detectedInTraces.contains(2L));
        Assertions.assertTrue(detectedInTraces.contains(3L));
        Assertions.assertFalse(detectedInTraces.contains(4L));

        Occurrences trace1 = ocs.stream().filter(x -> x.getTraceID() == 3).collect(Collectors.toList()).get(0);
        List<EventBoth> eventsMatch = trace1.getOccurrences().get(0).getOccurrence();
        Assertions.assertEquals(0, eventsMatch.get(0).getPosition());
        Assertions.assertEquals(5, eventsMatch.get(1).getPosition());
        Assertions.assertEquals(6, eventsMatch.get(2).getPosition());

        trace1 = ocs.stream().filter(x -> x.getTraceID() == 2).collect(Collectors.toList()).get(0);
        eventsMatch.clear();
        eventsMatch = trace1.getOccurrences().get(0).getOccurrence();
        Assertions.assertEquals(0, eventsMatch.get(0).getPosition());
        Assertions.assertEquals(6, eventsMatch.get(1).getPosition());
        Assertions.assertEquals(7, eventsMatch.get(2).getPosition());
    }
}
