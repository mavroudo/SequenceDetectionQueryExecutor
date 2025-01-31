package com.datalab.siesta.queryprocessor.declare.queryPlans.existence;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.model.EventN;
import com.datalab.siesta.queryprocessor.declare.model.EventPairSupport;
import com.datalab.siesta.queryprocessor.declare.model.ExistenceConstraint;
import com.datalab.siesta.queryprocessor.declare.model.declareState.ExistenceState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.UnorderStateI;
import com.datalab.siesta.queryprocessor.declare.model.declareState.UnorderStateU;
import com.datalab.siesta.queryprocessor.declare.queryPlans.QueryPlanState;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseExistence;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseExistenceState;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryExistenceWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import com.datalab.siesta.queryprocessor.declare.model.UnorderedHelper;
import com.datalab.siesta.queryprocessor.declare.model.PairConstraint;

import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;
import org.springframework.beans.factory.annotation.Autowired;



import scala.Tuple2;

@Component
@RequestScope
public class QueryPlanExistancesState extends QueryPlanState {

    private DBConnector dbConnector;

    @Autowired
    public QueryPlanExistancesState(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext, DBConnector dbConnector) {
        super(declareDBConnector, javaSparkContext);
        this.dbConnector = dbConnector;
    }

    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryExistenceWrapper qew = (QueryExistenceWrapper) qw;
        

        Broadcast<Long> bTraces = javaSparkContext.broadcast(metadata.getTraces());
        Broadcast<Double> bSupport = javaSparkContext.broadcast(qew.getSupport());

        //get all possible activities from database to create activity matrix
        List<String> activities = dbConnector.getEventNames(metadata.getLogname());
        JavaRDD<String> activityRDD = javaSparkContext.parallelize(activities);
        JavaPairRDD<String,String> activityMatrix = activityRDD.cartesian(activityRDD);
        
        QueryResponseExistenceState response = this.extractConstraintsFunction(bSupport, bTraces, activityMatrix, qew);

        this.extractStatistics(qew);
        response.setUpToDate(qew.isStateUpToDate());
        if(!qew.isStateUpToDate()){
            response.setEventsPercentage((qew.getIndexedEvents()/metadata.getEvents())*100);
            response.setTracesPercentage((qew.getIndexedTraces()/metadata.getTraces())*100);
            response.setMessage("State is not fully updated. Consider re-running the preprocess to get 100% accurate constraints");
        }else{
            response.setEventsPercentage(100);
            response.setTracesPercentage(100);
        }

        return response;
    }

    public QueryResponseExistenceState extractConstraintsFunction(Broadcast<Double> bSupport, Broadcast<Long> bTraces, JavaPairRDD<String,String> activityMatrix, 
            QueryExistenceWrapper qew){
        QueryResponseExistenceState response = new QueryResponseExistenceState();

        String[] existenceConstraints = {"existence","absence","exactly"};
        if(Arrays.stream(existenceConstraints).anyMatch(qew.getModes()::contains)){
            this.calculateExistence(qew.getModes(), response, bTraces, bSupport);
        }
        
        String[] unorderedConstraints = {"co-existence","not-co-existence", "choice",
                "exclusive-choice", "responded-existence"};
        if(Arrays.stream(unorderedConstraints).anyMatch(qew.getModes()::contains)){
            this.calculateUnordered(qew.getModes(), response, bTraces, bSupport, activityMatrix);
        }
        return response;
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

    private void calculateUnordered(List<String> modes, QueryResponseExistence qre, Broadcast<Long> bTraces, Broadcast<Double> bSupport, JavaPairRDD<String,String> activityMatrix){
        

        JavaRDD<UnorderStateI> iTable = declareDBConnector.queryUnorderStateI(metadata.getLogname());
        JavaRDD<UnorderStateU> uTable = declareDBConnector.queryUnorderStateU(metadata.getLogname());

        // Extract the unordered constraints by merging the activity matrix - iTable - uTable
        List<PairConstraint> unorderedConstraints = activityMatrix
            .keyBy((Function<Tuple2<String, String>, String>) x-> x._1())
            .leftOuterJoin(uTable.keyBy((Function<UnorderStateU, String>) x-> x.get_1()))
            .map(x -> {
                String eventA = x._2()._1()._1();
                String eventB = x._2()._1()._2();
                String key = eventA.compareTo(eventB) < 0 ? eventA + eventB : eventB + eventA;
                return new UnorderedHelper(eventA, eventB, x._2()._2().orElse(new UnorderStateU("", 0L)).get_2(), 0L, 0L, key);
            })
            .keyBy((Function<UnorderedHelper, String>) x-> x.getEventB())
            .leftOuterJoin(uTable.keyBy((Function<UnorderStateU, String>) x-> x.get_1()))
            .map(x->{
                return new UnorderedHelper(x._2()._1().getEventA(), x._2()._1().getEventB(), x._2()._1().getUa(), x._2()._2().orElse(new UnorderStateU("", 0L)).get_2(), 0L, x._2()._1().getKey());
            })
            .keyBy((Function<UnorderedHelper, String>) x-> x.getKey())
            .leftOuterJoin(iTable.keyBy((Function<UnorderStateI, String>) x-> x.get_1() + x.get_2()))
            .map(x -> {
                UnorderedHelper p = x._2()._1();
                return new UnorderedHelper(p.getEventA(), p.getEventB(), p.getUa(), p.getUb(), x._2()._2().orElse(new UnorderStateI("", "", 0L)).get_3(), p.getKey());
            })
            .distinct()
            .flatMap((FlatMapFunction<UnorderedHelper, PairConstraint>) x ->{
                List<PairConstraint> l = new ArrayList<>();
                long r = bTraces.getValue() - x.getUa() + x.getPairs();
                l.add(new PairConstraint(new EventPairSupport(x.getEventA(), x.getEventB(), r), "responded-existence"));
                if(x.getEventA().compareTo(x.getEventB()) < 0){
                    r = x.getUa() + x.getUb() - x.getPairs();
                    l.add(new PairConstraint(new EventPairSupport(x.getEventA(), x.getEventB(), r), "choice"));
                    r = bTraces.getValue() - x.getUa() - x.getUb() + 2 * x.getPairs();
                    l.add(new PairConstraint(new EventPairSupport(x.getEventA(), x.getEventB(), r), "co-existence"));
                    //exclusive_choice = total - co-existen
                    l.add(new PairConstraint(new EventPairSupport(x.getEventA(), x.getEventB(), bTraces.getValue()-r), "exclusive-choice"));
                    //not-existence : traces where a exist and not b, traces where b exists and not a, traces where neither occur
                    r = bTraces.getValue() - x.getPairs();
                    l.add(new PairConstraint(new EventPairSupport(x.getEventA(), x.getEventB(), r), "not-co-existence"));
                }
                return l.iterator();
            } )
            .filter((Function<PairConstraint, Boolean>)  x -> (x.getEventPairSupport().getSupport() / (double) bTraces.getValue()) >= bSupport.getValue())
            .map(x->{
                EventPairSupport eps = new EventPairSupport();
                eps.setEventA(x.getEventPairSupport().getEventA());
                eps.setEventB(x.getEventPairSupport().getEventB());
                eps.setSupport(x.getEventPairSupport().getSupport()/(double)bTraces.getValue());
                return new PairConstraint(eps, x.getRule());
            })
            .collect();

            if(modes.contains("responded-existence")){
                qre.setRespondedExistence(getUnorderToResponse("responded-existence", unorderedConstraints));
            }
            if(modes.contains("choice")){
                qre.setChoice(getUnorderToResponse("choice", unorderedConstraints));
            }
            if(modes.contains("co-existence")){
                qre.setCoExistence(getUnorderToResponse("co-existence", unorderedConstraints));
            }
            if(modes.contains("exclusive-choice")){
                qre.setExclusiveChoice(getUnorderToResponse("exclusive-choice", unorderedConstraints));
            }
            if(modes.contains("not-co-existence")){
                qre.setNotCoExistence(getUnorderToResponse("not-co-existence", unorderedConstraints));
            }
            
    }

    private List<EventPairSupport> getUnorderToResponse(String rule, List<PairConstraint> constraints){
        return constraints.stream().filter(x->{
            return x.getRule().equals(rule);
        })
        .map(x->{
            return x.getEventPairSupport();
        })
        .collect(Collectors.toList());
    }

}
