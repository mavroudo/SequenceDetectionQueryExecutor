package com.datalab.siesta.queryprocessor.controllers;

import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponsePatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.QueryExploration;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.QueryPatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.QueryStats;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryExploreWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryMetadataWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryStatsWrapper;
import com.datalab.siesta.queryprocessor.services.LoadInfo;
import com.datalab.siesta.queryprocessor.services.LoadedEventTypes;
import com.datalab.siesta.queryprocessor.services.LoadedMetadata;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.MappingJacksonValue;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;

/**
 * Main SIESTA controller
 */
@RestController
@RequestMapping(path = "/")
public class QueryResponseController {

    private LoadedMetadata allMetadata;
    private final QueryStats qs;
    private final QueryPatternDetection qpd;
    private final QueryExploration queryExploration;
    private final ObjectMapper objectMapper;
    private LoadedEventTypes loadedEventTypes;
    private final LoadInfo loadInfo;

    @Autowired
    public QueryResponseController(LoadedMetadata allMetadata, QueryStats qs,
                                   QueryPatternDetection qpd, QueryExploration queryExploration,
                                   ObjectMapper objectMapper, LoadedEventTypes loadedEventTypes, LoadInfo loadInfo) {
        this.allMetadata = allMetadata;
        this.qs = qs;
        this.qpd = qpd;
        this.queryExploration = queryExploration;
        this.objectMapper = objectMapper;
        this.loadedEventTypes = loadedEventTypes;
        this.loadInfo = loadInfo;
    }

    /**
     * Returns the name of all the indexed log databases
     */
    @RequestMapping(path = "/lognames", method = RequestMethod.GET)
    public ResponseEntity<MappingJacksonValue> getLognames() {
        MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(allMetadata.getMetadata().keySet());
        return new ResponseEntity<>(mappingJacksonValue, HttpStatus.OK);
    }

    /**
     * Returns the names of the different activities that belong to a specific log database
     */
    @RequestMapping(path="/eventTypes", method = RequestMethod.POST)
    public ResponseEntity<MappingJacksonValue> getEventTypes(@RequestBody QueryMetadataWrapper qmw) {
        String logname = qmw.getLog_name();
        List<String> s = loadedEventTypes.getEventTypes().getOrDefault(logname,null);
        if (s == null) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        } else {
            MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(s);
            return new ResponseEntity<>(mappingJacksonValue, HttpStatus.OK);
        }
    }

    /**
     * After the indexing of a new log file, this function will make sure that the required information
     * (like log databases and event types for each one) are fetched
     */
    @RequestMapping(path = "/refreshData", method = RequestMethod.GET)
    public ResponseEntity<MappingJacksonValue> refreshData() {
        this.loadedEventTypes=loadInfo.getAllEventTypes();
        this.allMetadata=loadInfo.getAllMetadata();
        return new ResponseEntity<>(HttpStatus.OK);
    }

    /**
     * Returns the metadata for a specific log database. Metadata include parameter values (like lookback, split_every_day)
     * and metadata (like number of traces, events and event-pairs).
     */
    @RequestMapping(path = "/metadata", method = RequestMethod.POST)
    public ResponseEntity<MappingJacksonValue> getMetadata(@RequestBody QueryMetadataWrapper qmw) {
        String logname = qmw.getLog_name();
        Metadata m = allMetadata.getMetadata(logname);
        if (m == null) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        } else {
            MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(m);
            return new ResponseEntity<>(mappingJacksonValue, HttpStatus.OK);
        }
    }

    /**
     * Returns the stats for each concecutive event-pair. For example for the pattern ABC, it will return the stats
     * for the event pairs A-B and B-C. Stats include min,max, average duration and number of completions.
     */
    @RequestMapping(path = "/stats", method = RequestMethod.POST)
    public ResponseEntity<MappingJacksonValue> getStats(@RequestBody QueryStatsWrapper qsp) {
        Metadata m = allMetadata.getMetadata(qsp.getLog_name());
        if (m == null) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        } else {
            QueryPlan qp = qs.createQueryPlan(qsp, m);
            QueryResponse qrs = qp.execute(qsp);
            MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(qrs);
            return new ResponseEntity<>(mappingJacksonValue, HttpStatus.OK);
        }
    }

    /**
     * Returns the trace ids and the specific locations where the query pattern has been detected.
     */
    @RequestMapping(path = "/detection", method = RequestMethod.POST)
    public ResponseEntity<String> patternDetection(@RequestBody QueryPatternDetectionWrapper qpdw) throws IOException {
        Metadata m = allMetadata.getMetadata(qpdw.getLog_name());
        if (m == null) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        } else {
            QueryPlan qp = qpd.createQueryPlan(qpdw, m);
            QueryResponse qrs = qp.execute(qpdw);
            if(qrs instanceof QueryResponsePatternDetection) {
                return new ResponseEntity<>(objectMapper.writeValueAsString(qrs), HttpStatus.OK);
            }else{
                return new ResponseEntity<>(objectMapper.writeValueAsString(qrs), HttpStatus.BAD_REQUEST);
            }
        }
    }

    /**
     * Returns the possible continuations of the query pattern, based on the stats that have been stored for each event
     * pair. There are 3 types of exploration, namely fast, accurate and hybrid. These types trade-off between
     * accuracy and response time.
     */
    @RequestMapping(path="/explore", method = RequestMethod.POST)
    public ResponseEntity<String> exploration(@RequestBody  QueryExploreWrapper queryExploreWrapper) throws IOException{
        Metadata m = allMetadata.getMetadata(queryExploreWrapper.getLog_name());
        if (m == null) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        } else {
            QueryPlan qp = queryExploration.createQueryPlan(queryExploreWrapper,m);
            QueryResponse qrs = qp.execute(queryExploreWrapper);
            return new ResponseEntity<>(objectMapper.writeValueAsString(qrs), HttpStatus.OK);
        }
    }
}
