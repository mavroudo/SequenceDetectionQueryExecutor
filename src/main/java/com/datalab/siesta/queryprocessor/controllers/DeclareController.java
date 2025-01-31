package com.datalab.siesta.queryprocessor.controllers;

import com.datalab.siesta.queryprocessor.declare.queries.QueryDeclareAll;
import com.datalab.siesta.queryprocessor.declare.queries.QueryExistence;
import com.datalab.siesta.queryprocessor.declare.queries.QueryOrderedRelations;
import com.datalab.siesta.queryprocessor.declare.queryPlans.QueryPlanDeclareAll;
import com.datalab.siesta.queryprocessor.declare.queryPlans.existence.QueryPlanExistences;
import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderedRelations;
import com.datalab.siesta.queryprocessor.declare.queries.QueryPositions;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseDeclareAll;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryExistenceWrapper;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryOrderRelationWrapper;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryPositionWrapper;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryWrapperDeclare;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.services.LoadedMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    private QueryDeclareAll queryDeclareAll;

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
     * @param enforceNormalMining if true, the normal mining will be enforced
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
            QueryWrapperDeclare qwd = this.createQueryWrapper(m, log_database, support, enforceNormalMining);
            QueryPositionWrapper qps = new QueryPositionWrapper(support);
            qps.setWrapper(qwd);
            QueryPlan queryPlanPosition = queryPositions.createQueryPlan(qps, m);
            QueryResponse qr = queryPlanPosition.execute(qps);
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
     * @param enforceNormalMining if true, the normal mining will be enforced
     */
    @RequestMapping(path = { "/existences", "/existences/" }, method = RequestMethod.GET)
    public ResponseEntity getExistenceConstraints(@RequestParam String log_database,
            @RequestParam(required = false, defaultValue = "0.9") double support,
            @RequestParam(required = false, defaultValue = "false") boolean enforceNormalMining,
            @RequestParam List<String> modes) throws IOException {
        Metadata m = allMetadata.getMetadata(log_database);
        if (m == null) {
            return new ResponseEntity<>("{\"message\":\"Log database is not found! If it is recently indexed " +
                    "consider executing endpoint /refresh \"", HttpStatus.NOT_FOUND);
        } else {
            QueryWrapperDeclare qwd = this.createQueryWrapper(m, log_database, support, enforceNormalMining);
            QueryExistenceWrapper qps = new QueryExistenceWrapper(support);
            qps.setWrapper(qwd);
            
            List<String> faultyModes = this.evaluateModes(modes);
            if (!faultyModes.isEmpty()) {
                return new ResponseEntity<>("Modes " + String.join(",", faultyModes) + " are incorrect",
                        HttpStatus.NOT_FOUND);
            }
            qps.setModes(modes);
            QueryPlan queryPlan = queryExistence.createQueryPlan(qps, m);
            QueryResponse qr = queryPlan.execute(qps);

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
     * @param enforceNormalMining if true, the normal mining will be enforced
     */
    @RequestMapping(path = { "/ordered-relations", "/ordered-relations/" }, method = RequestMethod.GET)
    public ResponseEntity getOrderedRelationsConstraints(@RequestParam String log_database,
            @RequestParam(required = false, defaultValue = "0.9") double support,
            @RequestParam(required = false, defaultValue = "false") boolean enforceNormalMining,
            @RequestParam(required = false, defaultValue = "simple") String mode,
            @RequestParam(required = false, defaultValue = "response") String constraint) throws IOException {
        Metadata m = allMetadata.getMetadata(log_database);
        if (m == null) {
            return new ResponseEntity<>("{\"message\":\"Log database is not found! If it is recently indexed " +
                    "consider executing endpoint /refresh \"", HttpStatus.NOT_FOUND);
        } else {
            QueryWrapperDeclare qwd = this.createQueryWrapper(m, log_database, support, enforceNormalMining);
            QueryOrderRelationWrapper queryOrderRelationWrapper = new QueryOrderRelationWrapper(support);
            queryOrderRelationWrapper.setWrapper(qwd);
            queryOrderRelationWrapper.setMode(mode);
            queryOrderRelationWrapper.setConstraint(constraint);

            QueryPlan qp = queryOrderedRelations.createQueryPlan(queryOrderRelationWrapper,m);
            QueryResponse qr = qp.execute(queryOrderRelationWrapper);
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
            @RequestParam(required = false, defaultValue = "0.9") double support,
            @RequestParam(required = false, defaultValue = "false") boolean enforceNormalMining) throws IOException {

        Metadata m = allMetadata.getMetadata(log_database);
        if (m == null) {
            return new ResponseEntity<>("{\"message\":\"Log database is not found! If it is recently indexed " +
                    "consider executing endpoint /refresh \"", HttpStatus.NOT_FOUND);
        } else {
            QueryWrapperDeclare qwd = this.createQueryWrapper(m, log_database, support, enforceNormalMining);
            QueryPlan qp = this.queryDeclareAll.createQueryPlan(qwd, m);
            QueryResponse response = qp.execute(qwd);
            return new ResponseEntity<>(objectMapper.writeValueAsString(response), HttpStatus.OK);
        }
    }

    private QueryWrapperDeclare createQueryWrapper(Metadata m, String log_database, double support, boolean enforceNormalMining){
        QueryWrapperDeclare qwd = new QueryWrapperDeclare(support);
        qwd.setLog_name(log_database);
        qwd.setEnforceNormalMining(enforceNormalMining);
        if(!m.getLast_declare_mined().equals("")){ //there index set
            qwd.setStateAvailable(true);
        }
        return qwd;
    }

    /**
     * Evaluates which of the provided modes are correct
     * @param modes list of provided modes
     * @return a list containing the valid modes
     */
    private List<String> evaluateModes(List<String> modes) {
        List<String> s = new ArrayList<>();
        List<String> l = Arrays.asList("existence", "absence", "exactly", "co-existence","not-co-existence", "choice",
                "exclusive-choice", "responded-existence");
        Set<String> correct_ms = new HashSet<>(l);
        for (String m : modes) {
            if (!correct_ms.contains(m)) {
                s.add(m);
            }
        }
        return s;
    }

}
