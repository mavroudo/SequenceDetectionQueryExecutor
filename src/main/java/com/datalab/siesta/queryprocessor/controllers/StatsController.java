package com.datalab.siesta.queryprocessor.controllers;

import com.datalab.siesta.queryprocessor.model.Queries.QueryStatsWrapper;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.MappingJacksonValue;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/")
public class StatsController {

    @Autowired
    private DBConnector dbConnector;

    @RequestMapping(path = "/stats",method = RequestMethod.GET)
    public ResponseEntity<MappingJacksonValue> getStats(@RequestBody QueryStatsWrapper qsp){
        System.out.println(qsp);

        return new ResponseEntity<>(HttpStatus.OK);
    }
}
