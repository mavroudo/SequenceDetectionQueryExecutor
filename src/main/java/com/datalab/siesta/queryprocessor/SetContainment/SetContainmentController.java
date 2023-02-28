package com.datalab.siesta.queryprocessor.SetContainment;

import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
@RequestMapping("/set-containment")
@ConditionalOnProperty(
        value = "database",
        havingValue = "cassandra-rdd"
)
public class SetContainmentController {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private QuerySetContainment querySetContainment;


    @RequestMapping(path = "detect", method = RequestMethod.GET)
    public ResponseEntity<String> detect(@RequestBody String json) throws IOException {
        QueryPatternDetectionWrapper qpdw = null;
        try {
            qpdw = objectMapper.readValue(json, QueryPatternDetectionWrapper.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        QueryPlan queryPlan = querySetContainment.createQueryPlan(qpdw,null);
        QueryResponse queryResponse = queryPlan.execute(qpdw);
        return new ResponseEntity<>(objectMapper.writeValueAsString(queryResponse), HttpStatus.OK);
    }
}
