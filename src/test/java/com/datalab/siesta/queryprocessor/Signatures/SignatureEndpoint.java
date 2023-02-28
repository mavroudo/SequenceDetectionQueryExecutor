package com.datalab.siesta.queryprocessor.Signatures;


import com.datalab.siesta.queryprocessor.model.Events.EventSymbol;
import com.datalab.siesta.queryprocessor.model.Patterns.ComplexPattern;
import com.datalab.siesta.queryprocessor.model.Patterns.SimplePattern;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.jupiter.api.Assertions;
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
public class SignatureEndpoint {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    private ComplexPattern getPattern() {
        EventSymbol ep1 = new EventSymbol("A", 0, "");
        EventSymbol ep2 = new EventSymbol("B", 1, "");
        List<EventSymbol> events = new ArrayList<>();
        events.add(ep1);
        events.add(ep2);
        return new ComplexPattern(events);
    }

    @Test
    void testSignatureEndpoint() throws Exception{
        String url = "http://localhost:8080/signatures/detect";
        QueryPatternDetectionWrapper qpdw = new QueryPatternDetectionWrapper();
        qpdw.setPattern(this.getPattern());
        qpdw.setLog_name("test");
        String s = objectMapper.writeValueAsString(qpdw);
        ResultActions trial =  mockMvc.perform(get(url).content(s).contentType(MediaType.APPLICATION_JSON));
        MvcResult results = trial.andExpect(status().isOk()).andReturn();
        String response = results.getResponse().getContentAsString();
        Assertions.assertNotNull(response);
    }
}
