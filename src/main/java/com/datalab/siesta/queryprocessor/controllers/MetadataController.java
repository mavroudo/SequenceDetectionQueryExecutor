package com.datalab.siesta.queryprocessor.controllers;

import com.datalab.siesta.queryprocessor.model.LoadedMetadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryMetadataWrapper;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.MappingJacksonValue;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;


import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping(path = "/")
public class MetadataController {

    @Autowired
    private DBConnector dbConnector;

    @Autowired
    private LoadedMetadata allMetadata;


    @RequestMapping(path = "/metadata",method = RequestMethod.GET)
    public ResponseEntity<MappingJacksonValue> getMetadata(@RequestBody QueryMetadataWrapper qmw) {
        String logname = qmw.getLog_name();
        if(allMetadata.getMetadata().containsKey(logname)) {
            MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(allMetadata.getMetadata().get(logname));
            return new ResponseEntity<>(mappingJacksonValue, HttpStatus.OK);
        }else{
            if (dbConnector.findAllLongNames().contains(logname)){
                MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(allMetadata.getMetadata().get(logname));
                return new ResponseEntity<>(mappingJacksonValue, HttpStatus.OK);
            }else{
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            }
        }
    }
}
