package com.datalab.siesta.queryprocessor.Signatures;

import com.datalab.siesta.queryprocessor.model.DBModel.EventTypes;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventSymbol;
import com.datalab.siesta.queryprocessor.model.Patterns.ComplexPattern;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest

class CassandraConnectionSignatureTest {

    @Autowired
    private CassandraConnectionSignature cassandraConnectionSignature;

    private ComplexPattern getPattern() {
        EventSymbol ep1 = new EventSymbol("A", 0, "");
        EventSymbol ep2 = new EventSymbol("B", 1, "");
        List<EventSymbol> events = new ArrayList<>();
        events.add(ep1);
        events.add(ep2);
        return new ComplexPattern(events);
    }
    @Test
    void getSignature() {
        Signature s = cassandraConnectionSignature.getSignature("test");
        Assertions.assertTrue(s.getEventPairs().size()>0);
        Assertions.assertTrue(s.getEvents().size()>0);
    }

    @Test
    void getPossibleTraces(){
        Signature s = cassandraConnectionSignature.getSignature("test");
        List<Long> possibleTraces = cassandraConnectionSignature.getPossibleTraceIds(this.getPattern(),"test",s);
        Assertions.assertTrue(possibleTraces.size()>0);
    }

    @Test
    void getOriginalTraces(){
        List<Long> traces = new ArrayList<>();
        traces.add(1L);
        traces.add(2L);
        traces.add(3L);
        Signature s = cassandraConnectionSignature.getSignature("test");
        Map<Long,List<Event>> response = cassandraConnectionSignature.getOriginalTraces(traces,"test");
        assertEquals(response.size(), traces.size());
    }

}