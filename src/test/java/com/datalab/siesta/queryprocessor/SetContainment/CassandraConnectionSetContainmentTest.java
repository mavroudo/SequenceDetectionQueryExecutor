package com.datalab.siesta.queryprocessor.SetContainment;

import com.datalab.siesta.queryprocessor.Signatures.Signature;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventSymbol;
import com.datalab.siesta.queryprocessor.model.Patterns.ComplexPattern;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class CassandraConnectionSetContainmentTest {
    @Autowired
    private CassandraConnectionSetContainment cassandraConnectionSetContainment;

    private ComplexPattern getPattern() {
        EventSymbol ep1 = new EventSymbol("A", 0, "");
        EventSymbol ep2 = new EventSymbol("B", 1, "");
        List<EventSymbol> events = new ArrayList<>();
        events.add(ep1);
        events.add(ep2);
        return new ComplexPattern(events);
    }

    @Test
    void getPossibleTraceIds() {
        List<Long> possibleTraces = cassandraConnectionSetContainment.getPossibleTraceIds(this.getPattern(),"test");
        Assertions.assertTrue(possibleTraces.size()>0);
    }

    @Test
    void getOriginalTraces() {
        List<Long> traces = new ArrayList<>();
        traces.add(1L);
        traces.add(2L);
        traces.add(3L);
        Map<Long,List<Event>> response = cassandraConnectionSetContainment.getOriginalTraces(traces,"test");
        assertEquals(response.size(), traces.size());
    }
}