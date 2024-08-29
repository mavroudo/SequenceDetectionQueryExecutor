package com.datalab.siesta.queryprocessor.controllers;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * Health Controller that it is used to check if the server is online.
 */
@RestController
@RequestMapping(path = "/health")
public class HealthCheckController {

    @RequestMapping(path = "/check",method = RequestMethod.GET)
    public ResponseEntity<String> healthcheck() {
        return new ResponseEntity<>("{ \"status\": \"ok\"}", HttpStatus.OK);
    }
}
