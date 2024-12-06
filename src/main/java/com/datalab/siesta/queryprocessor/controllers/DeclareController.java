package com.datalab.siesta.queryprocessor.controllers;

import com.datalab.siesta.queryprocessor.declare.queries.QueryExistence;
import com.datalab.siesta.queryprocessor.declare.queries.QueryOrderedRelations;
import com.datalab.siesta.queryprocessor.declare.queryPlans.QueryPlanDeclareAll;
import com.datalab.siesta.queryprocessor.declare.queryPlans.existence.QueryPlanExistences;
import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderedRelations;
import com.datalab.siesta.queryprocessor.declare.queries.QueryPositions;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseAll;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryPositionWrapper;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.services.LoadInfo;
import com.datalab.siesta.queryprocessor.services.LoadedEventTypes;
import com.datalab.siesta.queryprocessor.services.LoadedMetadata;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private LoadedMetadata allMetadata;

    @Autowired
    private QueryPositions queryPositions;

    @Autowired
    private QueryExistence queryExistence;

    @Autowired
    private QueryOrderedRelations queryOrderedRelations;

    @Autowired
    private QueryPlanDeclareAll queryPlanDeclareAll;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * Endpoint that extracts the position constraints
     * 
     * @param log_database the log database
     * @param position     can be 'first' -> for the first,'last'-> for the last and
     *                     'both' for both first and last
     * @param support      minimum support that a pattern should have in order to be
     *                     added to the result set
     */
    @RequestMapping(path = { "/positions", "/positions/" }, method = RequestMethod.GET)
    public ResponseEntity getPositionConstraints(@RequestParam String log_database,
            @RequestParam(required = false, defaultValue = "both") String position,
            @RequestParam(required = false, defaultValue = "0.9") double support,
            @RequestParam(required = false, defaultValue = "false") boolean enforceNormalMining) throws IOException {
        Metadata m = allMetadata.getMetadata(log_database);
        if (m == null) {
            return new ResponseEntity<>("{\"message\":\"Log database is not found! If it is recently indexed " +
                    "consider executing endpoint /refresh \"", HttpStatus.NOT_FOUND);
        } else {
            QueryPositionWrapper qpw = new QueryPositionWrapper(support);
            qpw.setLog_name(log_database);
            qpw.setMode(position);
            qpw.setEnforceNormalMining(enforceNormalMining);
            // TODO: add the other statistics to determine the accuracy of the returned
            // constraints

            QueryPlan queryPlanPosition = queryPositions.createQueryPlan(qpw, m);
            QueryResponse qr = queryPlanPosition.execute(qpw);
            return new ResponseEntity<>(objectMapper.writeValueAsString(qr), HttpStatus.OK);
        }
    }

    /**
     * Endpoint that extracts the existence constraints
     * 
     * @param log_database the log database
     * @param support      minimum support that a pattern should have in order to be
     *                     added to the result set
     * @param modes        a list of all the templates that needs to be extracted
     *                     (e.g., 'existence', 'exactly' etc.)
     *                     a complete list can be found in
     *                     {@link QueryPlanExistences#execute(String, List, double)}
     */
    @RequestMapping(path = { "/existences", "/existences/" }, method = RequestMethod.GET)
    public ResponseEntity getExistenceConstraints(@RequestParam String log_database,
            @RequestParam(required = false, defaultValue = "0.9") double support,
            @RequestParam List<String> modes) throws IOException {
        Metadata m = allMetadata.getMetadata(log_database);
        if (m == null) {
            return new ResponseEntity<>("{\"message\":\"Log database is not found! If it is recently indexed " +
                    "consider executing endpoint /refresh \"", HttpStatus.NOT_FOUND);
        } else {
            QueryPlanExistences queryPlanExistences = queryExistence.getQueryPlan(m);
            List<String> faultyModes = queryPlanExistences.evaluateModes(modes);
            if (!faultyModes.isEmpty()) {
                return new ResponseEntity<>("Modes " + String.join(",", faultyModes) + " are incorrect",
                        HttpStatus.NOT_FOUND);
            }
            QueryResponse qr = queryPlanExistences.execute(log_database, modes, support);
            return new ResponseEntity<>(objectMapper.writeValueAsString(qr), HttpStatus.OK);
        }
    }

    /**
     * Endpoint that extracts the order relation constraints
     * 
     * @param log_database the log database
     * @param support      minimum support that a pattern should have in order to be
     *                     added to the result set
     * @param mode         can be set to 'simple', 'alternate' or 'chain' for the
     *                     different order relationships supported
     *                     by DECLARE
     * @param constraint   can be set to 'response', 'precedence' or 'succession'
     *                     (which will also calculate the
     *                     no succession constraints)
     */
    @RequestMapping(path = { "/ordered-relations", "/ordered-relations/" }, method = RequestMethod.GET)
    public ResponseEntity getOrderedRelationsConstraints(@RequestParam String log_database,
            @RequestParam(required = false, defaultValue = "0.9") double support,
            @RequestParam(required = false, defaultValue = "simple") String mode,
            @RequestParam(required = false, defaultValue = "response") String constraint) throws IOException {
        Metadata m = allMetadata.getMetadata(log_database);
        if (m == null) {
            return new ResponseEntity<>("{\"message\":\"Log database is not found! If it is recently indexed " +
                    "consider executing endpoint /refresh \"", HttpStatus.NOT_FOUND);
        } else {
            QueryPlanOrderedRelations queryPlanOrderedRelations = queryOrderedRelations.getQueryPlan(m, mode);
            QueryResponse qr = queryPlanOrderedRelations.execute(log_database, constraint, support);
            return new ResponseEntity<>(objectMapper.writeValueAsString(qr), HttpStatus.OK);
        }
    }

    /**
     * Efficiently combines the previously 3 endpoints to extract all DECLARE
     * patterns from a log database
     * 
     * @param log_database the log database
     * @param support      minimum support that a pattern should have in order to be
     *                     added to the result set
     */
    @RequestMapping(path = "/", method = RequestMethod.GET)
    public ResponseEntity getAll(@RequestParam String log_database,
            @RequestParam(required = false, defaultValue = "0.9") double support) throws IOException {

        Metadata m = allMetadata.getMetadata(log_database);
        if (m == null) {
            return new ResponseEntity<>("{\"message\":\"Log database is not found! If it is recently indexed " +
                    "consider executing endpoint /refresh \"", HttpStatus.NOT_FOUND);
        } else {
            this.queryPlanDeclareAll.setMetadata(m);
            QueryResponseAll queryResponseAll = this.queryPlanDeclareAll.execute(log_database, support);
            return new ResponseEntity<>(objectMapper.writeValueAsString(queryResponseAll), HttpStatus.OK);
        }
    }

}
