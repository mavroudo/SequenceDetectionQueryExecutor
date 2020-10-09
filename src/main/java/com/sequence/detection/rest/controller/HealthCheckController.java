package com.sequence.detection.rest.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @author Andreas Kosmatopoulos
 */
@RestController
@RequestMapping(path="/health")
public class HealthCheckController {

    /**
     * Returns a health check response
     * @return a {@link org.springframework.http.ResponseEntity} containing the health check response
     */
    @RequestMapping(value = "/healthcheck", method = RequestMethod.GET)
    public ResponseEntity healthcheck () {
        return new ResponseEntity<>("{ \"status\": \"ok\"}", HttpStatus.OK);
    }
}
