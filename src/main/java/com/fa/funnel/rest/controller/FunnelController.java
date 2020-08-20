package com.fa.funnel.rest.controller;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.fa.funnel.rest.model.*;
import com.fa.funnel.rest.query.ResponseBuilder;
import com.fa.funnel.rest.spring.exception.BadRequestException;
import com.fa.funnel.rest.util.Utilities;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.MappingJacksonValue;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.*;

/**
 * The FunnelController class receives (through the JSON API) a user-provided funnel along with its method of execution.
 * In the end it produces a JSON output (through Jackson Mapping) to be used by the calling interface
 * See the Confluence pages for more details (https://followanalytics.atlassian.net/wiki/spaces/FOL/pages/48005264/Funnels+-+Engine+API+reference)
 * @author Andreas Kosmatopoulos
 */
@RestController
@RequestMapping(path="/funnel")
public class FunnelController
{
    @Autowired
    private CassandraOperations cassandraOperations;

    @Autowired
    private Environment environment;

    @RequestMapping(value = "/detect", method = RequestMethod.POST)
    public ResponseEntity<MappingJacksonValue>
    detect(@RequestParam(value = "from", required = false, defaultValue = "1970-01-01") String from,
               @RequestParam(value = "till", required = false, defaultValue = "") String till,
               @RequestBody FunnelWrapper funnelWrapper)
    {
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

        DetectionResponse response = responseBuilder.buildDetectionResponse();

        MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(response);

        return new ResponseEntity<>(mappingJacksonValue, HttpStatus.OK);

    }

    /**
     * Returns a JSON output corresponding to the {@code /quick_stats} endpoint
     * @param from Funnel start date (yyyy-MM-dd)
     * @param till Funnel end date (yyyy-MM-dd)
     * @param funnelWrapper Funnel wrapped in a {@code funnel = &#123; ... &#125;} tag
     * @return a {@link org.springframework.http.ResponseEntity} containing the JSON output
     */
    @RequestMapping(value = "/quick_stats", method = RequestMethod.POST)
    public ResponseEntity<MappingJacksonValue>
            quickStats(@RequestParam(value = "from", required = false, defaultValue = "1970-01-01") String from,
                       @RequestParam(value = "till", required = false, defaultValue = "") String till,
                       @RequestBody FunnelWrapper funnelWrapper)
    {
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
     * @param mode The Explore mode. Can be "fast", "hybrid" or "accurate"
     * @param from Funnel start date (yyyy-MM-dd)
     * @param till Funnel end date (yyyy-MM-dd)
     * @param position The position index on the funnel on which the explore is performed (Default is equal to the length of the funnel)
     * @param appID If an appID is specified, restrict the results to only contain continuations of the specified appID
     * @param top Hybrid mode only. After executing a "fast" explore, perform an "accurate" explore on the specified number of sorted results
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
                    @RequestBody FunnelWrapper funnelWrapper)
    {
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
                response = responseBuilder.buildExploreResponse(position, appID);
                break;
            case "hybrid":
                response = responseBuilder.buildExploreResponseHybrid(position, appID, top);
                break;
            default: // fast
                response = responseBuilder.buildExploreResponseFast(position, appID);
                break;

        }

        MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(response);

        return new ResponseEntity<>(mappingJacksonValue, HttpStatus.OK);

    }

    /**
     * Exports detailed completions for a particular file on a CSV file
     * @param response The main body on which the response will be presented
     * @param from Funnel start date (yyyy-MM-dd)
     * @param till Funnel end date (yyyy-MM-dd)
     * @param funnelWrapper Funnel wrapped in a {@code funnel = &#123; ... &#125;} tag
     * @throws Exception If the csv file cannot be manipulated
     */
    @RequestMapping(value = "/export_completions", method = RequestMethod.POST)
    public void exportCompletions(HttpServletResponse response,
                       @RequestParam(value = "from", required = false, defaultValue = "1970-01-01") String from,
                       @RequestParam(value = "till", required = false, defaultValue = "") String till,
                       @RequestBody FunnelWrapper funnelWrapper) throws Exception
    {
        if (!Utilities.isValidDate(from))
            throw new BadRequestException("Wrong parameter: from (start) date!");

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

        String fileName =  responseBuilder.buildCSVFile(Paths.get("").toAbsolutePath().toString() + "/export/");

        System.out.println(Paths.get("").toAbsolutePath().toString());
        System.out.println(fileName);

        File file = new File(Paths.get("").toAbsolutePath() + "/export/" +fileName);

        System.out.println(file.getAbsolutePath());

        response.setContentType("text/csv");

        /* "Content-Disposition : inline" will show viewable types [like images/text/pdf/anything viewable by browser] right on browser
            while others(zip e.g) will be directly downloaded [may provide save as popup, based on your browser setting.]*/
        response.setHeader("Content-Disposition", String.format("inline; filename=\"" + file.getName() +"\""));


        /* "Content-Disposition : attachment" will be directly download, may provide save as popup, based on your browser setting*/
        //response.setHeader("Content-Disposition", String.format("attachment; filename=\"%s\"", file.getName()));

        response.setContentLength((int)file.length());

        InputStream inputStream = new BufferedInputStream(new FileInputStream(file));

        //Copy bytes from source to destination(outputstream in this example), closes both streams.
        FileCopyUtils.copy(inputStream, response.getOutputStream());
    }

    /**
     * Returns the status of the index built so far (i.e. indexed dates)
     * @return a {@link org.springframework.http.ResponseEntity} containing the JSON output
     */
    @RequestMapping(value = "/status", method = RequestMethod.GET)
    public ResponseEntity<MappingJacksonValue> status()
    {
        ResultSet indexStatus = cassandraOperations.getSession().execute(new SimpleStatement("SELECT * FROM index_status;"));
        List<Row> rows = indexStatus.all();
        Map<String, String> deviceMap = new HashMap<>();
        Map<String, String> userMap = new HashMap<>();
        for (Row row : rows) {
            if (row.getString("type_index").equals("user")) {
                userMap.put(row.getString("day"), row.getString("timestamp"));
            } else {
                deviceMap.put(row.getString("day"), row.getString("timestamp"));
            }
        }

        // right now don't use timestamp
        List<String> deviceKeys = new ArrayList<>(deviceMap.keySet());
        Collections.sort(deviceKeys);
        List<String> userKeys = new ArrayList<>(userMap.keySet());
        Collections.sort(userKeys);

        MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(new Status(userKeys, deviceKeys));

        return new ResponseEntity<>(mappingJacksonValue, HttpStatus.OK);
    }
}
