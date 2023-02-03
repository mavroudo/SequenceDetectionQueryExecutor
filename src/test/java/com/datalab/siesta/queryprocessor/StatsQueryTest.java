package com.datalab.siesta.queryprocessor;

import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Patterns.SimplePattern;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryStatsWrapper;
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
public class StatsQueryTest {

    @Autowired
    private MockMvc mockMvc;


    @Test
    public void getStats() throws Exception{
        String url = "http://localhost:8080/stats";
        SimplePattern p = new SimplePattern();
        EventPos e = new EventPos("A_Accepted",0);
        EventPos e2 = new EventPos("A_Create Application",1);
        List<EventPos> events = new ArrayList<>();
        events.add(e);
        events.add(e2);
        p.setEvents(events);
        QueryStatsWrapper qsw = new QueryStatsWrapper();
        qsw.setPattern(p);
        qsw.setLog_name("bpi_2017");
        ObjectMapper Obj = new ObjectMapper();
        String s = Obj.writeValueAsString(qsw);
        ResultActions trial = mockMvc.perform(get(url).content(s).contentType(MediaType.APPLICATION_JSON));
        MvcResult results = trial.andExpect(status().isOk()).andReturn();
        String response = results.getResponse().getContentAsString();
    }
}
