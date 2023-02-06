package com.datalab.siesta.queryprocessor;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraint;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraint;
import com.datalab.siesta.queryprocessor.model.Events.EventSymbol;
import com.datalab.siesta.queryprocessor.model.Patterns.ComplexPattern;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;

import java.util.ArrayList;
import java.util.List;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
public class PatternDetectionQueryTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void getStats() throws Exception{
        String url = "http://localhost:8080/detection";
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
        QueryPatternDetectionWrapper qsw = new QueryPatternDetectionWrapper();
        qsw.setPattern(p);
        qsw.setLog_name("bpi_2017");
        ObjectMapper Obj = new ObjectMapper();
        String s = Obj.writeValueAsString(qsw);
        System.out.println(s);

        ObjectMapper obj2 = new ObjectMapper();
        QueryPatternDetectionWrapper q = obj2.readValue(s, QueryPatternDetectionWrapper.class);
        ResultActions trial = mockMvc.perform(get(url).content(s).contentType(MediaType.APPLICATION_JSON));
        MvcResult results = trial.andExpect(status().isOk()).andReturn();
        String response = results.getResponse().getContentAsString();
    }
}
