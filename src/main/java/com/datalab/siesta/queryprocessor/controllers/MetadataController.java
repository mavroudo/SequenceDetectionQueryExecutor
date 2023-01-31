package com.datalab.siesta.queryprocessor.controllers;

import com.datalab.siesta.queryprocessor.model.Metadata;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.MappingJacksonValue;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/")
public class MetadataController {

    @Autowired
    private DBConnector dbConnector;


    @RequestMapping(path = "/metadata",method = RequestMethod.GET)
    public ResponseEntity<MappingJacksonValue> healthcheck() {
        Metadata m= dbConnector.getMetadata("bpi_2017");
        MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(m);
        return new ResponseEntity<>(mappingJacksonValue, HttpStatus.OK);
    }
}
