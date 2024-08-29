package com.datalab.siesta.queryprocessor.controllers;


import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * TODO: description
 */
@RestController
@RequestMapping(path = "/patterns")
public class PatternAnalysisController {

    @Autowired
    private DBConnector dbConnector;

    @RequestMapping(path = "/violations",method = RequestMethod.GET)
    public ResponseEntity<String> getViolatingPatterns(@RequestParam(defaultValue = "test") String log_database) {
        StringBuilder response = new StringBuilder();
        for (Count c : dbConnector.getEventPairs(log_database)) {
            response.append(c.getEventA());
            response.append(c.getEventB());
            response.append("\n");
        }
        return new ResponseEntity<>(response.toString(), HttpStatus.OK);
    }
}
