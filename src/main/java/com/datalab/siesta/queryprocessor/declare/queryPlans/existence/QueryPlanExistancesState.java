package com.datalab.siesta.queryprocessor.declare.queryPlans.existence;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.model.EventN;
import com.datalab.siesta.queryprocessor.declare.model.ExistenceConstraint;
import com.datalab.siesta.queryprocessor.declare.model.declareState.ExistenceState;
import com.datalab.siesta.queryprocessor.declare.queryPlans.QueryPlanState;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseExistence;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryExistenceWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;
import org.springframework.beans.factory.annotation.Autowired;


import scala.Tuple2;

@Component
@RequestScope
public class QueryPlanExistancesState extends QueryPlanState {

    @Autowired
    public QueryPlanExistancesState(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext) {
        super(declareDBConnector, javaSparkContext);
    }

    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryExistenceWrapper qew = (QueryExistenceWrapper) qw;
        Broadcast<Long> bTraces = javaSparkContext.broadcast(metadata.getTraces());
        Broadcast<Double> bSupport = javaSparkContext.broadcast(qew.getSupport());
        QueryResponseExistence qre = new QueryResponseExistence();

        String[] existenceConstraints = {"existence","absence","exactly"};
        if(Arrays.stream(existenceConstraints).anyMatch(qew.getModes()::contains)){
            this.calculateExistence(qew.getModes(), qre, bTraces, bSupport);
        }

         
        return qre;

    }

    private void calculateExistence(List<String> modes, QueryResponseExistence qre, Broadcast<Long> bTraces, Broadcast<Double> bSupport){
        JavaRDD<ExistenceState> existences = declareDBConnector.queryExistenceState(metadata.getLogname());
        JavaRDD<ExistenceConstraint> c = existences.groupBy((Function<ExistenceState, String>) x -> {
            return x.getEvent_type();
        })
        .flatMap((FlatMapFunction<Tuple2<String, Iterable<ExistenceState>>, ExistenceConstraint>) x -> {
            List<ExistenceConstraint> l = new ArrayList<>();
            String eventType = x._1;
            Iterable<ExistenceState> activities = x._2;
            // Convert Iterable to List for sorting
            List<ExistenceState> sortedActivities = new ArrayList<>();
            activities.forEach(sortedActivities::add);
            sortedActivities.sort(Comparator.comparingInt(ExistenceState::getOccurrences));

            // Exactly constraint
            for (ExistenceState activity : sortedActivities) {
                if ((double) activity.getContained() / bTraces.getValue() >= bSupport.getValue()) {
                    l.add(new ExistenceConstraint("exactly", eventType, activity.getOccurrences(),
                            (double) activity.getContained() / bTraces.getValue()));
                }
            }

            //Existence Constraints
            Collections.reverse(sortedActivities); //reverse the list so higher is lower
            long cumulativeOccurrences = sortedActivities.get(0).getContained();
            int pos = 0;
            //this loop will start from the higher number of occurrences and go down to one. In each iteration
            //will calculate the number of traces that contain at least x instances of the activity 
            for(int occurrences =  sortedActivities.get(0).getOccurrences();occurrences>=1;occurrences--){
                //the occurrences are equal to the next one smaller occurrence
                if(pos+1<sortedActivities.size() && occurrences == sortedActivities.get(pos+1).getOccurrences()){
                    pos+=1;
                    cumulativeOccurrences+= sortedActivities.get(pos).getContained();
                }
                if ((double) cumulativeOccurrences / bTraces.getValue() >= bSupport.getValue()) {
                    l.add(new ExistenceConstraint("existence", eventType, occurrences,
                            (double) cumulativeOccurrences / bTraces.getValue()));
                }
            }

            // Absence constraint
            Collections.reverse(sortedActivities); // smaller to greater
            pos=-1;
            int cumulativeAbsence = (int) (bTraces.getValue() - sortedActivities.stream().mapToLong(ExistenceState::getContained).sum());
            for (int absence = 1; absence<=3;absence++){
                if ((double) cumulativeAbsence / bTraces.getValue() >= bSupport.getValue()) {
                    l.add(new ExistenceConstraint("absence", eventType, absence,
                            (double) cumulativeAbsence / bTraces.getValue()));
                }
                if(pos+1<sortedActivities.size() && absence == sortedActivities.get(pos+1).getOccurrences()){
                    pos+=1;
                    cumulativeAbsence+= sortedActivities.get(pos).getContained();
                }

            }
            return l.iterator();
        });
        List<ExistenceConstraint> constraints = c.collect();
        if(modes.contains("existence")){
            qre.setExistence(getExistenceToResponse("existence", constraints));
        }
        if(modes.contains("absence")){
            qre.setAbsence(getExistenceToResponse("absence", constraints));
        }
        if(modes.contains("exactly")){
            qre.setExactly(getExistenceToResponse("exactly", constraints));
        }
    }

    private List<EventN> getExistenceToResponse(String rule, List<ExistenceConstraint> constraints){
        return constraints.stream().filter(x->{
            return x.getRule().equals(rule);
        }).map(x->{
            return new EventN(x.getEvent_type(), x.getN(), x.getOccurrences());
        }).collect(Collectors.toList());
    }

}
