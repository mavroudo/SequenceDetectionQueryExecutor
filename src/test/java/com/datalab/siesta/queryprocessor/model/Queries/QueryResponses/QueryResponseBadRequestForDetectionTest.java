package com.datalab.siesta.queryprocessor.model.Queries.QueryResponses;

import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.ExtractedPairsForPatternDetection;
import com.datalab.siesta.queryprocessor.model.Patterns.SimplePattern;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.http.converter.json.MappingJacksonValue;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class QueryResponseBadRequestForDetectionTest {

    @Test
    public void testQueryResponseFormat() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        EventPos ep1 = new EventPos("T0",0);
        EventPos ep2 = new EventPos("T1",1);
        EventPos ep3 = new EventPos("T2",2);
        EventPos ep4 = new EventPos("T3",3);
        List<EventPos> events = new ArrayList<>();
        events.add(ep1);
        events.add(ep2);
        events.add(ep3);
        events.add(ep4);
        SimplePattern p = new SimplePattern();
        p.setEvents(events);
        ExtractedPairsForPatternDetection pairs = p.extractPairsForPatternDetection(false);
        QueryResponseBadRequestForDetection qrbd = new QueryResponseBadRequestForDetection();
        qrbd.setNonExistingPairs(new ArrayList<>(pairs.getAllPairs()));
        MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(qrbd);
        String s1 = mapper.writeValueAsString(mappingJacksonValue.getValue());


        qrbd = new QueryResponseBadRequestForDetection();
        qrbd.setConstraintsNotFulfilled(new ArrayList<>(pairs.getAllPairs()));
        mappingJacksonValue = new MappingJacksonValue(qrbd);

        String s2 = mapper.writeValueAsString(mappingJacksonValue.getValue());

    }

}