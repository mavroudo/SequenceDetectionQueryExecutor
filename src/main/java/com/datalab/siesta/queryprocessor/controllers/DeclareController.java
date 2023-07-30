package com.datalab.siesta.queryprocessor.controllers;


import com.datalab.siesta.queryprocessor.declare.queryPlans.QueryPlanPosition;
import com.datalab.siesta.queryprocessor.declare.queryPlans.QueryPositions;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.services.LoadInfo;
import com.datalab.siesta.queryprocessor.services.LoadedEventTypes;
import com.datalab.siesta.queryprocessor.services.LoadedMetadata;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping(path = "/declare")
public class DeclareController {

    @Autowired
    private DBConnector dbConnector;

    @Autowired
    private LoadedMetadata allMetadata;

    @Autowired
    private LoadedEventTypes loadedEventTypes;

    @Autowired
    private LoadInfo loadInfo;

    @Autowired
    private QueryPositions queryPositions;

    @Autowired
    private ObjectMapper objectMapper;

    @RequestMapping(path = "/first", method = RequestMethod.GET)
    public ResponseEntity getFirsts(@RequestParam String log_database,
                                    @RequestParam(required = false, defaultValue = "both") String position,
                                    @RequestParam(required = false, defaultValue = "0.9") double support) throws IOException {
        Metadata m = allMetadata.getMetadata(log_database);
        if (m == null) {
            return new ResponseEntity<>("{\"message\":\"Log database is not found! If it is recently indexed " +
                    "consider executing endpoint /refresh \"",HttpStatus.NOT_FOUND);
        }else{
            QueryPlanPosition queryPlanPosition = queryPositions.getQueryPlan(position,m);
            QueryResponse qr = queryPlanPosition.execute(log_database, support);
            return new ResponseEntity<>(objectMapper.writeValueAsString(qr), HttpStatus.OK);
        }

    }


}
