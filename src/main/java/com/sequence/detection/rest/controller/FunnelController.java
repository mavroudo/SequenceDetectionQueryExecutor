package com.sequence.detection.rest.controller;

import com.sequence.detection.rest.model.*;
import com.sequence.detection.rest.model.Responses.DetectionResponse;
import com.sequence.detection.rest.model.Responses.DetectionResponseAllInstances;
import com.sequence.detection.rest.query.ResponseBuilder;
import com.sequence.detection.rest.model.Responses.DetectionResponseNoTime;
import com.sequence.detection.rest.signatures.SignaturesResponseBuilder;
import com.sequence.detection.rest.spring.exception.BadRequestException;
import com.sequence.detection.rest.triplets.TripletsResponseBuilder;
import com.sequence.detection.rest.util.ParseGroups;
import com.sequence.detection.rest.util.Utilities;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.MappingJacksonValue;
import org.springframework.web.bind.annotation.*;
import com.sequence.detection.rest.setcontainment.SetContainmentResponseBuilder;

import java.util.List;
import java.util.Set;

/**
 * The FunnelController class receives (through the JSON API) a user-provided funnel along with its method of execution.
 * In the end it produces a JSON output (through Jackson Mapping) to be used by the calling interface
 *
 * @author Andreas Kosmatopoulos
 */
@RestController
@RequestMapping(path = "/funnel")
public class FunnelController {
    @Autowired
    private CassandraOperations cassandraOperations;

    @Autowired
    private Environment environment;

    @RequestMapping(value = "/detect-triplets",method = RequestMethod.POST)
    public ResponseEntity<MappingJacksonValue>
    detectTriplet(@RequestParam(value = "from", required = false, defaultValue = "1970-01-01") String from,
                  @RequestParam(value = "till", required = false, defaultValue = "") String till,
                  @RequestParam(value = "false", required = false) boolean return_all,
                  @RequestBody FunnelWrapper funnelWrapper){
        if (!Utilities.isValidDate(from))
            throw new BadRequestException("Wrong parameter: from (start) date!");

        if (till.isEmpty()) {
            till = Utilities.getToday();
        } else {
            if (!Utilities.isValidDate(till))
                throw new BadRequestException("Wrong parameter: until (end) date!");
        }
        Funnel funnel = funnelWrapper.getFunnel();
        funnel.setMaxDuration(funnel.getMaxDuration() * 1000);
        System.out.println("From date: " + from);
        System.out.println("Till date: " + till);
        System.out.println(funnel);



        TripletsResponseBuilder responseBuilder = new TripletsResponseBuilder(cassandraOperations.getSession().getCluster(),
                cassandraOperations.getSession(),
                cassandraOperations.getSession().getCluster().getMetadata().getKeyspace(environment.getProperty("cassandra_keyspace")),
                environment.getProperty("cassandra_keyspace"),
                funnel, from, till,return_all
        );
        DetectionResponseAllInstances response = responseBuilder.buildDetectionResponse();
        MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(response);
        return new ResponseEntity<>(mappingJacksonValue, HttpStatus.OK);
    }

    @RequestMapping(value = "/detect", method = RequestMethod.POST)
    public ResponseEntity<MappingJacksonValue>
    detect(@RequestParam(value = "from", required = false, defaultValue = "1970-01-01") String from,
           @RequestParam(value = "till", required = false, defaultValue = "") String till,
           @RequestParam(value = "group_by", required = false, defaultValue = "") String group_by,
           @RequestParam(value = "grouping_method", required = false, defaultValue = "smart") String grouping,
           @RequestParam(value = "return_all",required = false,defaultValue = "false") String returnAll,
           @RequestParam(value = "optimization", required = false,defaultValue = "lfc") String optimization,
           //lfc -> least frequent consecutive, lf -> least frequent from stnm, gsc -> set cover
           @RequestBody FunnelWrapper funnelWrapper) {
        if (!Utilities.isValidDate(from))
            throw new BadRequestException("Wrong parameter: from (start) date!");

        if (till.isEmpty()) {
            till = Utilities.getToday();
        } else {
            if (!Utilities.isValidDate(till))
                throw new BadRequestException("Wrong parameter: until (end) date!");
        }

        boolean return_all = Boolean.parseBoolean(returnAll);

        Funnel funnel = funnelWrapper.getFunnel();
        funnel.setMaxDuration(funnel.getMaxDuration() * 1000);
        System.out.println("From date: " + from);
        System.out.println("Till date: " + till);
        System.out.println(funnel);

        ResponseBuilder responseBuilder = new ResponseBuilder(cassandraOperations.getSession().getCluster(),
                cassandraOperations.getSession(),
                cassandraOperations.getSession().getCluster().getMetadata().getKeyspace(environment.getProperty("cassandra_keyspace")),
                environment.getProperty("cassandra_keyspace"),
                funnel, from, till
        );
        List<Set<Integer>> groups = ParseGroups.parse(group_by);
        if(groups==null){
            throw new BadRequestException("Wrong format: group_by cannot be parsed");
        }
        DetectionResponseAllInstances response;
        if(groups.isEmpty()){
            response = responseBuilder.buildDetectionResponse(optimization,return_all);
        }else{
            response = responseBuilder.buildGroupsDetectionResponse(groups,grouping,return_all);
        }
        MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(response);
        return new ResponseEntity<>(mappingJacksonValue, HttpStatus.OK);

    }


    @RequestMapping(value = "/signature", method = RequestMethod.POST)
    public ResponseEntity<MappingJacksonValue>
    signatures(@RequestParam(value = "from", required = false, defaultValue = "1970-01-01") String from,
               @RequestParam(value = "till", required = false, defaultValue = "") String till,
               @RequestParam(value = "strategy", required = false, defaultValue = "skiptillnextmatch") String strategy,
               @RequestBody FunnelWrapper funnelWrapper) {
        if (!Utilities.isValidDate(from))
            throw new BadRequestException("Wrong parameter: from (start) date!");

        if (till.isEmpty()) {
            till = Utilities.getToday();
        } else {
            if (!Utilities.isValidDate(till))
                throw new BadRequestException("Wrong parameter: until (end) date!");
        }

        Funnel funnel = funnelWrapper.getFunnel();
        funnel.setMaxDuration(funnel.getMaxDuration() * 1000);
        System.out.println("From date: " + from);
        System.out.println("Till date: " + till);
        System.out.println(funnel);

        SignaturesResponseBuilder responseBuilder = new SignaturesResponseBuilder(cassandraOperations.getSession().getCluster(),
                cassandraOperations.getSession(),
                cassandraOperations.getSession().getCluster().getMetadata().getKeyspace(environment.getProperty("cassandra_keyspace")),
                environment.getProperty("cassandra_keyspace"),
                funnel, from, till, strategy
        );
        DetectionResponseNoTime response = responseBuilder.buildDetectionResponseNoTime();
        MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(response);
        return new ResponseEntity<>(mappingJacksonValue, HttpStatus.OK);
    }

    @RequestMapping(value = "/setcontainment", method = RequestMethod.POST)
    public ResponseEntity<MappingJacksonValue>
    setContainment(@RequestParam(value = "from", required = false, defaultValue = "1970-01-01") String from,
                   @RequestParam(value = "till", required = false, defaultValue = "") String till,
                   @RequestParam(value = "strategy", required = false, defaultValue = "skiptillnextmatch") String strategy,
                   @RequestBody FunnelWrapper funnelWrapper) {
        if (!Utilities.isValidDate(from))
            throw new BadRequestException("Wrong parameter: from (start) date!");

        if (till.isEmpty()) {
            till = Utilities.getToday();
        } else {
            if (!Utilities.isValidDate(till))
                throw new BadRequestException("Wrong parameter: until (end) date!");
        }

        Funnel funnel = funnelWrapper.getFunnel();
        funnel.setMaxDuration(funnel.getMaxDuration() * 1000);
        System.out.println("From date: " + from);
        System.out.println("Till date: " + till);
        System.out.println(funnel);

        SetContainmentResponseBuilder responseBuilder = new SetContainmentResponseBuilder(cassandraOperations.getSession().getCluster(),
                cassandraOperations.getSession(),
                cassandraOperations.getSession().getCluster().getMetadata().getKeyspace(environment.getProperty("cassandra_keyspace")),
                environment.getProperty("cassandra_keyspace"),
                funnel, from, till, strategy
        );

//        DetectionResponseNoTime response = responseBuilder.buildDetectionResponseNoTime();

        MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(responseBuilder.buildDetectionResponseNoTime());

        return new ResponseEntity<>(mappingJacksonValue, HttpStatus.OK);
    }

    /**
     * Returns a JSON output corresponding to the {@code /quick_stats} endpoint
     *
     * @param from          Funnel start date (yyyy-MM-dd)
     * @param till          Funnel end date (yyyy-MM-dd)
     * @param funnelWrapper Funnel wrapped in a {@code funnel = &#123; ... &#125;} tag
     * @return a {@link org.springframework.http.ResponseEntity} containing the JSON output
     */
    @RequestMapping(value = "/quick_stats", method = RequestMethod.POST)
    public ResponseEntity<MappingJacksonValue>
    quickStats(@RequestParam(value = "from", required = false, defaultValue = "1970-01-01") String from,
               @RequestParam(value = "till", required = false, defaultValue = "") String till,
               @RequestBody FunnelWrapper funnelWrapper) {
        if (!Utilities.isValidDate(from))
            throw new BadRequestException("Wrong parameter: from (start) date!");

        if (till.isEmpty()) {
            till = Utilities.getToday();
        } else {
            if (!Utilities.isValidDate(till))
                throw new BadRequestException("Wrong parameter: until (end) date!");
        }

        Funnel funnel = funnelWrapper.getFunnel();
        funnel.setMaxDuration(funnel.getMaxDuration() * 1000);
        System.out.println("From date: " + from);
        System.out.println("Till date: " + till);
        System.out.println(funnel);

        ResponseBuilder responseBuilder = new ResponseBuilder(cassandraOperations.getSession().getCluster(),
                cassandraOperations.getSession(),
                cassandraOperations.getSession().getCluster().getMetadata().getKeyspace(environment.getProperty("cassandra_keyspace")),
                environment.getProperty("cassandra_keyspace"),
                funnel, from, till
        );

        QuickStatsResponse response = responseBuilder.buildQuickStatsResponse();

        MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(response);

        return new ResponseEntity<>(mappingJacksonValue, HttpStatus.OK);

    }

    /**
     * Returns a JSON output corresponding to the {@code /explore/&#123;mode&#125;} endpoint
     *
     * @param mode          The Explore mode. Can be "fast", "hybrid" or "accurate"
     * @param from          Funnel start date (yyyy-MM-dd)
     * @param till          Funnel end date (yyyy-MM-dd)
     * @param position      The position index on the funnel on which the explore is performed (Default is equal to the length of the funnel)
     * @param appID         If an appID is specified, restrict the results to only contain continuations of the specified appID
     * @param top           Hybrid mode only. After executing a "fast" explore, perform an "accurate" explore on the specified number of sorted results
     * @param funnelWrapper Funnel wrapped in a {@code funnel = &#123; ... &#125;} tag
     * @return a {@link org.springframework.http.ResponseEntity} containing the JSON output
     */
    @RequestMapping(value = "/explore/{mode}", method = RequestMethod.POST)
    public ResponseEntity<MappingJacksonValue>
    explore(@PathVariable String mode,
            @RequestParam(value = "from", required = false, defaultValue = "1970-01-01") String from,
            @RequestParam(value = "till", required = false, defaultValue = "") String till,
            @RequestParam(value = "position", required = false, defaultValue = "") String position,
            @RequestParam(value = "app_id", required = false, defaultValue = "") String appID,
            @RequestParam(value = "top", required = false, defaultValue = "10") String top,
            @RequestBody FunnelWrapper funnelWrapper) {
        if (!(mode.equals("fast") || mode.equals("hybrid") || mode.equals("accurate")))
            throw new BadRequestException("Wrong explore mode");

        if (!Utilities.isValidDate(from))
            throw new BadRequestException("Wrong parameter: from (start) date!");
        if (!position.isEmpty() && !Utilities.isValidInteger(position))
            throw new BadRequestException("Wrong parameter: position!");
        if (!top.isEmpty() && !Utilities.isValidInteger(top))
            throw new BadRequestException("Wrong parameter: top!");

        if (till.isEmpty()) {
            till = Utilities.getToday();
        } else {
            if (!Utilities.isValidDate(till))
                throw new BadRequestException("Wrong parameter: until (end) date!");
        }

        Funnel funnel = funnelWrapper.getFunnel();
        System.out.println("From date: " + from);
        System.out.println("Till date: " + till);
        System.out.println(funnel);

        ResponseBuilder responseBuilder = new ResponseBuilder(cassandraOperations.getSession().getCluster(),
                cassandraOperations.getSession(),
                cassandraOperations.getSession().getCluster().getMetadata().getKeyspace(environment.getProperty("cassandra_keyspace")),
                environment.getProperty("cassandra_keyspace"),
                funnel, from, till
        );

        ExploreResponse response = null;

        if (position.isEmpty()) {
            position = Integer.toString(funnelWrapper.getFunnel().getSteps().size());
        }

        switch (mode) {
            case "accurate":
                response = responseBuilder.buildExploreResponse(position);
                break;
            case "hybrid":
                response = responseBuilder.buildExploreResponseHybrid(position, top);
                break;
            default: // fast
                response = responseBuilder.buildExploreResponseFast(position);
                break;

        }

        MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(response);

        return new ResponseEntity<>(mappingJacksonValue, HttpStatus.OK);

    }

}
