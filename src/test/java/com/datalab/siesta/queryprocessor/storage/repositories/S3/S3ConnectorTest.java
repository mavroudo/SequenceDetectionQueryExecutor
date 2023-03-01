package com.datalab.siesta.queryprocessor.storage.repositories.S3;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraint;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraint;
import com.datalab.siesta.queryprocessor.model.DBModel.*;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventSymbol;
import com.datalab.siesta.queryprocessor.model.GroupConfig;
import com.datalab.siesta.queryprocessor.model.Patterns.ComplexPattern;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class S3ConnectorTest {

    @Autowired
    private S3Connector s3Connector;

    @Test
    void queryMetadata(){
        Metadata m = s3Connector.getMetadata("test");
        Assertions.assertEquals(23L,m.getEvents());
        Assertions.assertEquals("timestamps",m.getMode());
        Assertions.assertTrue(m.getHas_previous_stored());
        m = s3Connector.getMetadata("test_pos");
        Assertions.assertEquals(23L,m.getEvents());
        Assertions.assertEquals("positions",m.getMode());
        Assertions.assertTrue(m.getHas_previous_stored());
    }

    @Test
    void queryIndexTable(){
        Metadata m = s3Connector.getMetadata("test_pos");
        Set<EventPair> pairs = new HashSet<>();
        pairs.add(new EventPair(new Event("A"),new Event("A")));
        pairs.add(new EventPair(new Event("A"),new Event("B")));
        pairs.add(new EventPair(new Event("A"),new Event("C")));
        pairs.add(new EventPair(new Event("B"),new Event("A")));
        pairs.add(new EventPair(new Event("B"),new Event("B")));
        pairs.add(new EventPair(new Event("B"),new Event("C")));
        pairs.add(new EventPair(new Event("C"),new Event("A")));
        pairs.add(new EventPair(new Event("C"),new Event("B")));
        pairs.add(new EventPair(new Event("C"),new Event("C")));
        IndexRecords  ir = s3Connector.queryIndexTable(pairs,m.getLogname(),m);

        Map<EventTypes,List<IndexPair>> r =  ir.getRecords();
        // <B,A>
        Assertions.assertEquals(2,r.get(new EventTypes("B","A")).size());
        List<Long> contained =  r.get(new EventTypes("C","B")).stream().map(IndexPair::getTraceId).collect(Collectors.toList());
        Assertions.assertTrue(contained.contains(2L));
        // <C,B>
        Assertions.assertEquals(2,r.get(new EventTypes("C","B")).size());
        contained.clear();
        contained =  r.get(new EventTypes("C","B")).stream().map(IndexPair::getTraceId).collect(Collectors.toList());
        Assertions.assertTrue(contained.contains(2L));
        Assertions.assertTrue(contained.contains(3L));
        // <C,C>
        Assertions.assertEquals(2,r.get(new EventTypes("C","C")).size());
        contained.clear();
        contained =  r.get(new EventTypes("C","C")).stream().map(IndexPair::getTraceId).collect(Collectors.toList());
        Assertions.assertTrue(contained.contains(2L));
        Assertions.assertTrue(contained.contains(3L));
        // <A,A>
        Assertions.assertEquals(6,r.get(new EventTypes("A","A")).size());
        contained.clear();
        contained =  r.get(new EventTypes("A","A")).stream().map(IndexPair::getTraceId).collect(Collectors.toList());
        Assertions.assertTrue(contained.contains(2L));
        Assertions.assertTrue(contained.contains(3L));
        Assertions.assertTrue(contained.contains(1L));
        Assertions.assertTrue(contained.contains(4L));
        // <A,B>
        Assertions.assertEquals(5,r.get(new EventTypes("A","B")).size());
        contained.clear();
        contained =  r.get(new EventTypes("A","B")).stream().map(IndexPair::getTraceId).collect(Collectors.toList());
        Assertions.assertTrue(contained.contains(2L));
        Assertions.assertTrue(contained.contains(3L));
        Assertions.assertTrue(contained.contains(1L));
        // <B,C>
        Assertions.assertEquals(5,r.get(new EventTypes("B","C")).size());
        contained.clear();
        contained =  r.get(new EventTypes("B","C")).stream().map(IndexPair::getTraceId).collect(Collectors.toList());
        Assertions.assertTrue(contained.contains(2L));
        Assertions.assertTrue(contained.contains(3L));
        Assertions.assertTrue(contained.contains(1L));
        // <A,C>
        Assertions.assertEquals(5,r.get(new EventTypes("A","C")).size());
        contained.clear();
        contained =  r.get(new EventTypes("A","C")).stream().map(IndexPair::getTraceId).collect(Collectors.toList());
        Assertions.assertTrue(contained.contains(2L));
        Assertions.assertTrue(contained.contains(3L));
        Assertions.assertTrue(contained.contains(1L));
        Assertions.assertTrue(contained.contains(4L));
        // <C,A>
        Assertions.assertEquals(2,r.get(new EventTypes("C","A")).size());
        contained.clear();
        contained =  r.get(new EventTypes("C","A")).stream().map(IndexPair::getTraceId).collect(Collectors.toList());
        Assertions.assertTrue(contained.contains(2L));
        Assertions.assertTrue(contained.contains(4L));
        // <B,B>
        Assertions.assertEquals(3,r.get(new EventTypes("B","B")).size());
        contained.clear();
        contained =  r.get(new EventTypes("B","B")).stream().map(IndexPair::getTraceId).collect(Collectors.toList());
        Assertions.assertTrue(contained.contains(2L));
        Assertions.assertTrue(contained.contains(3L));
    }

    @Test
    void querySeqTable() {
        EventSymbol es1 = new EventSymbol("A_Accepted",0,"");
        EventSymbol es2 = new EventSymbol("A_Create Application",1,"");
        EventSymbol es3 = new EventSymbol("A_Concept",2,"");
        EventSymbol es4 = new EventSymbol("A_Submitted",3,"");
        List<EventSymbol> events = new ArrayList<>();
        events.add(es1);
        events.add(es2);
        events.add(es3);
        events.add(es4);
        TimeConstraint tc = new TimeConstraint(1,2,700);
        GapConstraint gc = new GapConstraint(2,3,3);
        List<Constraint> lc = new ArrayList<>();
        lc.add(tc);
        lc.add(gc);
        ComplexPattern p = new ComplexPattern(events);
        p.setConstraints(lc);
        p.setEventsWithSymbols(events);
        List<Long> l1= new ArrayList<>();
        l1.add(1L);
        l1.add(2L);
        Map<Long,List<EventBoth>> traces = s3Connector.querySeqTable("bpi_2017",l1);
        Map<Long,List<EventBoth>> traces2 = s3Connector.querySeqTable("bpi_2017",l1,p.getEventTypes(),null,null);
        System.out.println("hey");


    }

    @Test
    void querySingleTable(){
        Set<Long> traces = new HashSet<>();
        traces.add(1L);
        traces.add(2L);
        Set<String> eventTypes = new HashSet<>();
        eventTypes.add("A");
        eventTypes.add("B");
        List<EventBoth> events = s3Connector.querySingleTable("test",traces,eventTypes);
        Assertions.assertTrue(events.size()>0);
        List<Long> traceIds = events.stream().map(EventBoth::getTraceID).collect(Collectors.toList());
        Assertions.assertTrue(traceIds.containsAll(traces));
        Assertions.assertTrue(events.stream().map(EventBoth::getName).collect(Collectors.toList()).containsAll(eventTypes));
    }

    @Test
    void querySingleTableGroups(){
        GroupConfig groupConfig = new GroupConfig("[(1-3),(4)]");
        Set<String> eventTypes = new HashSet<>();
        eventTypes.add("A");
        eventTypes.add("B");
        Map<Integer,List<EventBoth>> events = s3Connector.querySingleTableGroups("test",groupConfig.getGroups(),eventTypes);
        assertEquals(1, events.size());
        List<Long> traceIds1 = events.get(1).stream().map(EventBoth::getTraceID).collect(Collectors.toList());
        Assertions.assertTrue(traceIds1.contains(1L));
        Assertions.assertTrue(traceIds1.contains(2L));
        Assertions.assertTrue(traceIds1.contains(3L));
        Assertions.assertFalse(traceIds1.contains(4L));

        groupConfig = new GroupConfig("[(1-2),(3)]");
        eventTypes.clear();
        eventTypes.add("A");
        eventTypes.add("B");
        events = s3Connector.querySingleTableGroups("test",groupConfig.getGroups(),eventTypes);

        assertEquals(2, events.size());
        traceIds1 = events.get(1).stream().map(EventBoth::getTraceID).collect(Collectors.toList());
        List<Long> traceIds2 = events.get(2).stream().map(EventBoth::getTraceID).collect(Collectors.toList());
        Assertions.assertTrue(traceIds1.contains(1L));
        Assertions.assertTrue(traceIds1.contains(2L));
        Assertions.assertFalse(traceIds1.contains(3L));
        Assertions.assertFalse(traceIds1.contains(4L));
        Assertions.assertTrue(traceIds2.contains(3L));


    }

    @Test
    void queryCountsForExploration() {
        List<Count> counts =s3Connector.getCountForExploration("test","A");
        Assertions.assertEquals(4,counts.size());
    }
}