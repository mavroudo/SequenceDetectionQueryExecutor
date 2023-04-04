package com.datalab.siesta.queryprocessor;


import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraint;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.EventSymbol;
import com.datalab.siesta.queryprocessor.model.Patterns.ComplexPattern;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
//import org.codehaus.jackson.map.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.util.Assert;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
public class TestDetectionEndpoint {

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void testEndpoint() throws Exception {
        String url = "http://localhost:8080/detection";
        ComplexPattern cp = new ComplexPattern();
        List<EventSymbol> es = new ArrayList<>();
        es.add(new EventSymbol("A",0,"_"));
        es.add(new EventSymbol("B",0,"_"));
        QueryPatternDetectionWrapper qp = new QueryPatternDetectionWrapper();

        qp.setLog_name("synthetic");
        qp.setFrom(new Timestamp(System.currentTimeMillis()));
        List<Constraint> cs = new ArrayList<>();
        cs.add(new TimeConstraint(0,1,10));
        cp.setConstraints(cs);

        qp.setPattern(cp);

        ObjectMapper Obj = new ObjectMapper();
        String s = Obj.writeValueAsString(qp);


        ResultActions trial = mockMvc.perform(post(url).content(s).contentType(MediaType.APPLICATION_JSON));
        MvcResult results = trial.andExpect(status().isOk()).andReturn();
        Metadata m = new ObjectMapper().readValue(results.getResponse().getContentAsString(), Metadata.class);


    }
}
