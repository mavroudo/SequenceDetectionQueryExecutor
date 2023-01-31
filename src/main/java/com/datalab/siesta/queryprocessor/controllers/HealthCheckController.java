package com.datalab.siesta.queryprocessor.controllers;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/health")
public class HealthCheckController {

    @RequestMapping(path = "/check",method = RequestMethod.GET)
    public ResponseEntity<String> healthcheck() {
        return new ResponseEntity<>("{ \"status\": \"ok\"}", HttpStatus.OK);
    }
}
