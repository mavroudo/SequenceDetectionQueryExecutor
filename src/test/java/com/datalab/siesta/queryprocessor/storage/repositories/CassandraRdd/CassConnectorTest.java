package com.datalab.siesta.queryprocessor.storage.repositories.CassandraRdd;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexRecords;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.storage.repositories.S3.S3Connector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class CassConnectorTest {

    @Autowired
    private CassConnector cassConnector;
    @Test
    void getMetadata() {
        Metadata m = cassConnector.getMetadata("test");
        Assertions.assertEquals(23L,m.getEvents());
        Assertions.assertEquals("timestamps",m.getMode());
        Assertions.assertTrue(m.getHas_previous_stored());
        m = cassConnector.getMetadata("test_pos");
        Assertions.assertEquals(23L,m.getEvents());
        Assertions.assertEquals("positions",m.getMode());
        Assertions.assertTrue(m.getHas_previous_stored());
    }

    @Test
    void findAllLongNames() {
        Set<String> lognames = cassConnector.findAllLongNames();
        Assertions.assertNotNull(lognames);
    }

    @Test
    void getCounts() {
        Set<EventPair> pairs = new HashSet<>();
        pairs.add(new EventPair(new Event("A"),new Event("A")));
        pairs.add(new EventPair(new Event("A"),new Event("B")));
        List<Count> c = cassConnector.getCounts("test",pairs);
        Assertions.assertNotNull(c);
        Assertions.assertEquals(2,c.size());
    }

    @Test
    void getEventNames(){
        List<String> events = cassConnector.getEventNames("test");
        Assertions.assertEquals(4,events.size());
    }

    @Test
    void querySeqTable(){
        List<Long> traces = new ArrayList<>(){{add(1L);add(2L);}};
        Set<String> events = new HashSet<>(){{add("A");add("B");}};
        Map<Long, List<EventBoth>>map = cassConnector.querySeqTable("test",traces,events,null,null);
        Assertions.assertEquals(2,map.size());
    }

    @Test
    void queryIndexTable(){
        Set<EventPair> pairs = new HashSet<>();
        pairs.add(new EventPair(new Event("A"),new Event("A")));
        pairs.add(new EventPair(new Event("A"),new Event("B")));
        Metadata m = cassConnector.getMetadata("test");
        IndexRecords ir = cassConnector.queryIndexTable(pairs,"test",m);
        Assertions.assertNotNull(ir);
        Assertions.assertEquals(2,ir.getRecords().size());
    }

    @Test
    void querySingleTable(){
        Set<Long> traces = new HashSet<>();
        traces.add(1L);
        traces.add(2L);
        Set<String> events = new HashSet<>();
        events.add("A");
        events.add("B");
        cassConnector.querySingleTable("test",traces,events);
    }

    @Test
    void queryCountsForExploration() {
        List<Count> counts =cassConnector.getCountForExploration("test","A");
        Assertions.assertEquals(4,counts.size());
    }




}