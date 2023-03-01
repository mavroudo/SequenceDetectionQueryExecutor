package com.datalab.siesta.queryprocessor.storage.repositories.S3;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraint;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexRecords;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Events.EventSymbol;
import com.datalab.siesta.queryprocessor.model.Patterns.ComplexPattern;
import com.datalab.siesta.queryprocessor.model.Patterns.SimplePattern;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

@SpringBootTest
public class DatabaseTest {

    @Autowired
    private S3Connector s3Connector;


    @Test
    public void readFromSeqTable(){
        List<Long> trace_ids = new ArrayList<>();
        trace_ids.add(1L);
        trace_ids.add(15L);
        Map<Long,List<EventBoth>> results = s3Connector.querySeqTable("bpi_2017_pos",trace_ids);
        Assertions.assertFalse(results.isEmpty());
        Assertions.assertEquals(trace_ids.size(),results.size());

        List<String> events = new ArrayList<>();
        events.add("A_Accepted");
        events.add("A_Create Application");
        events.add("A_Concept");
        events.add("A_Submitted");

        Map<Long,List<EventBoth>> results2 = s3Connector.querySeqTable("bpi_2017",trace_ids,new HashSet<>(events),null,null);
        Assertions.assertFalse(results.isEmpty());
        Assertions.assertEquals(results.size(),trace_ids.size());
        List<String> worked = results2.get(15L).stream()
                .map(Event::getName)
                .filter(y->!events.contains(y)).collect(Collectors.toList());
        Assertions.assertEquals(0,worked.size());

        //get large number of traces
        List<Long> trace_ids2 =LongStream.range(1,10000).boxed().collect(Collectors.toList());
        long start = System.currentTimeMillis();
        Map<Long,List<EventBoth>> results3 = s3Connector.querySeqTable("bpi_2017",trace_ids2);
        System.out.printf("Got results in %d seconds%n",(System.currentTimeMillis()-start)/1000);
        Assertions.assertFalse(results.isEmpty());
        Assertions.assertEquals(results.size(),trace_ids.size());
    }

    @Test
    public void readFromIndexTable(){
        String logname = "bpi_2017";
        Metadata m = s3Connector.getMetadata(logname);
        EventPos es1 = new EventPos("A_Accepted",0);
        EventPos es2 = new EventPos("A_Create Application",1);
        EventPos es3 = new EventPos("A_Concept",2);
        EventPos es4 = new EventPos("A_Submitted",3);
        List<EventPos> events = new ArrayList<>();
        events.add(es1);
        events.add(es2);
        events.add(es3);
        events.add(es4);
        SimplePattern p = new SimplePattern(events);
        //check the different formats
        IndexRecords indexRecords = s3Connector.queryIndexTable(p.extractPairsForPatternDetection(false)._2,logname,m);

        Assertions.assertNotNull(indexRecords);
    }




}
