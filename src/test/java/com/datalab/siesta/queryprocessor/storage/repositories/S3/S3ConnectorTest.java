package com.datalab.siesta.queryprocessor.storage.repositories.S3;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraint;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraint;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Events.EventSymbol;
import com.datalab.siesta.queryprocessor.model.Patterns.ComplexPattern;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class S3ConnectorTest {

    @Autowired
    private S3Connector s3Connector;

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
        Map<Long,List<EventBoth>> traces2 = s3Connector.querySeqTable("bpi_2017",l1,p.getEventTypes());
        System.out.println("hey");


    }
}