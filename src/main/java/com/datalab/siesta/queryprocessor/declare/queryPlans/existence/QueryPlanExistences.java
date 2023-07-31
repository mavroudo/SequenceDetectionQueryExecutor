package com.datalab.siesta.queryprocessor.declare.queryPlans.existence;

import com.datalab.siesta.queryprocessor.controllers.DeclareController;
import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.model.EventN;
import com.datalab.siesta.queryprocessor.declare.model.UniqueTracesPerEventType;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseExistence;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueryPlanExistences {

    private DeclareDBConnector declareDBConnector;
    private JavaSparkContext javaSparkContext;
    //initialize a protected variable of the required events
    private QueryResponseExistence queryResponseExistence;
    private Metadata metadata;


    @Autowired
    public QueryPlanExistences(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext) {
        this.declareDBConnector = declareDBConnector;
        this.javaSparkContext = javaSparkContext;
        this.queryResponseExistence = new QueryResponseExistence();
    }

    public QueryResponse execute(String logname, List<String> modes, double support) {

        //if existence, absence or exactly in modes
        JavaRDD<UniqueTracesPerEventType> uEventType = declareDBConnector.querySingleTableDeclare(logname);
        uEventType.persist(StorageLevel.MEMORY_AND_DISK());
        Map<String, HashMap<Integer, Long>> groupTimes = this.createMapForSingle(uEventType);
        existence(groupTimes,support, metadata.getTraces());
        absence(groupTimes,support, metadata.getTraces());
        exactly(groupTimes,support, metadata.getTraces());
        declareDBConnector.queryIndexTableDeclare(logname);
        return this.queryResponseExistence;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    private Map<String, HashMap<Integer, Long>> createMapForSingle(JavaRDD<UniqueTracesPerEventType> uEventType) {
        Map<String, HashMap<Integer, Long>> groupTimes = uEventType.map(x -> new Tuple2<>(x.getEventType(), x.groupTimes()))
                .keyBy(x -> x._1)
                .mapValues(x -> x._2)
                .collectAsMap();
        return groupTimes;
    }

    private void existence(Map<String, HashMap<Integer, Long>> groupTimes, double support, long totalTraces) {
        Set<String> eventTypes = groupTimes.keySet();
        List<EventN> response = new ArrayList<>();
        for (String et : eventTypes) { //for every event type
            HashMap<Integer, Long> t = groupTimes.get(et);
            List<Integer> times = new ArrayList<>(t.keySet()).stream().sorted(Comparator.reverseOrder())
                    .collect(Collectors.toList());
            for (int time : times) {
                double s = (double) times.stream().filter(x -> x >= time).mapToLong(t::get).sum()  / totalTraces;
                if (s >= support) {
                    response.add(new EventN(et, time, s));
                    break;
                }
            }
        }
        this.queryResponseExistence.setExistence(response);
    }

    private void absence(Map<String, HashMap<Integer, Long>> groupTimes, double support, long totalTraces) {
        Set<String> eventTypes = groupTimes.keySet();
        List<EventN> response = new ArrayList<>();
        for (String et : eventTypes) { //for every event type
            HashMap<Integer, Long> t = groupTimes.get(et);
            long totalSum = t.values().stream().mapToLong(x->x).sum();
            t.put(0,totalTraces-totalSum);
            List<Integer> times = new ArrayList<>(t.keySet()).stream().sorted().collect(Collectors.toList());
            if(!times.contains(2)) times.add(2); //to be sure that it will run at least once
            for (int time : times) {
                if(time <2 ){
                    continue;
                }
                double s = (double) times.stream().filter(x -> x < time).mapToLong(t::get).sum() / totalTraces;
                if (s >= support) {
                    response.add(new EventN(et, time, s));
                    break;
                }
            }
        }
        this.queryResponseExistence.setAbsence(response);
    }

    private void exactly(Map<String, HashMap<Integer, Long>> groupTimes, double support, long totalTraces) {
        Set<String> eventTypes = groupTimes.keySet();
        List<EventN> response = new ArrayList<>();
        for (String et : eventTypes) { //for every event type
            HashMap<Integer, Long> t = groupTimes.get(et);
            long totalSum = t.values().stream().mapToLong(x -> x).sum();
            if(!t.containsKey(0))  t.put(0, totalTraces - totalSum);
            for (Map.Entry<Integer, Long> x : t.entrySet()) {
                if (x.getValue() >= (support * totalTraces)) {
                    response.add(new EventN(et, x.getKey(), x.getValue().doubleValue() / totalTraces));
                }
            }
        }
        this.queryResponseExistence.setExactly(response);
    }


}
