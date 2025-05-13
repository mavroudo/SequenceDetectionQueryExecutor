package com.datalab.siesta.queryprocessor;

import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
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

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
public class MetadataQueryTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void getMetadata() throws Exception {
        String url = "http://localhost:8080/metadata";
        String q1 = "{\"log_name\":\"bpi_2017\"}";
        ResultActions trial = mockMvc.perform(get(url).content(q1).contentType(MediaType.APPLICATION_JSON));
        MvcResult results = trial.andExpect(status().isOk()).andReturn();
        Metadata m = new ObjectMapper().readValue(results.getResponse().getContentAsString(), Metadata.class);
        Assert.isTrue(m.getCompression() != null,"");
        Assert.isTrue(m.getEvents() > 0,"");
        Assert.isTrue(m.getPairs() > 0,"");

    }


}
