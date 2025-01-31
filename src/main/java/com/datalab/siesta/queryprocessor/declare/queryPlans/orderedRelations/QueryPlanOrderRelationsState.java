package com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.model.EventPairSupport;
import com.datalab.siesta.queryprocessor.declare.model.EventSupport;
import com.datalab.siesta.queryprocessor.declare.model.PairConstraint;
import com.datalab.siesta.queryprocessor.declare.model.declareState.NegativeState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.OrderState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.UnorderStateU;
import com.datalab.siesta.queryprocessor.declare.queryPlans.QueryPlanState;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseOrderedRelationsState;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryOrderRelationWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;


import scala.Tuple2;
import scala.Tuple3;

@Component
@RequestScope
public class QueryPlanOrderRelationsState extends QueryPlanState {

     private DBConnector dbConnector;

    public QueryPlanOrderRelationsState(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext,
            DBConnector dbConnector) {
        super(declareDBConnector,javaSparkContext);
        this.dbConnector = dbConnector;
    }

    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryOrderRelationWrapper qow = (QueryOrderRelationWrapper) qw;

        //create activity matrix from the event names
        List<String> activities = dbConnector.getEventNames(metadata.getLogname());
        JavaRDD<String> activityRDD = javaSparkContext.parallelize(activities);
        JavaPairRDD<String,String> activityMatrix = activityRDD.cartesian(activityRDD);

        //Extract All constraints
        Broadcast<Double> bSupport = this.javaSparkContext.broadcast(qow.getSupport());
        List<PairConstraint> constraints = this.extractAll(bSupport, activityMatrix);

        //filter the constraints to create the response
        QueryResponseOrderedRelationsState response = new QueryResponseOrderedRelationsState();
        this.setSpecificConstraints(qow, constraints, response);
        
        this.extractStatistics(qow);
        response.setUpToDate(qow.isStateUpToDate());
        if(!qow.isStateUpToDate()){
            response.setEventsPercentage((qow.getIndexedEvents()/metadata.getEvents())*100);
            response.setTracesPercentage((qow.getIndexedTraces()/metadata.getTraces())*100);
            response.setMessage("State is not fully updated. Consider re-running the preprocess to get 100% accurate constraints");
        }else{
            response.setEventsPercentage(100);
            response.setTracesPercentage(100);
        }

        return response;
    }

    public QueryResponseOrderedRelationsState extractConstraintFunction(List<PairConstraint> constraints, String mode){
        QueryResponseOrderedRelationsState response = new QueryResponseOrderedRelationsState();
        response.setMode(mode);
        for(PairConstraint uc:constraints){
            if (mode.equals("simple")){
                if(uc.getRule().equals("response")){  
                    response.getResponse().add(uc.getEventPairSupport());
                }else if(uc.getRule().equals("precedence") ){
                    response.getPrecedence().add(uc.getEventPairSupport());
                }else if(uc.getRule().equals("succession")){
                    response.getSuccession().add(uc.getEventPairSupport());
                }else if(uc.getRule().equals("not-succession")){
                    response.getNotSuccession().add(uc.getEventPairSupport());
                }
            }else{
                if(uc.getRule().contains("response") && uc.getRule().contains(mode)){  
                    response.getResponse().add(uc.getEventPairSupport());
                }else if(uc.getRule().contains("precedence") && uc.getRule().contains(mode) ){
                    response.getPrecedence().add(uc.getEventPairSupport());
                }else if(uc.getRule().contains("succession") && uc.getRule().contains(mode)){
                    response.getSuccession().add(uc.getEventPairSupport());
                }
                if(mode.equals("alternate") && uc.getRule().equals("not-succession")){
                    response.getNotSuccession().add(uc.getEventPairSupport());
                }else if(mode.equals("chain") && uc.getRule().equals("not-chain-succession")){
                    response.getNotSuccession().add(uc.getEventPairSupport());
                }
            }
            
        }
        return response;

    }

    public List<PairConstraint> extractAll(Broadcast<Double> bSupport, JavaPairRDD<String,String> activityMatrix){
        //load order constraints from the database
        JavaRDD<OrderState> orderStateRDD = this.declareDBConnector.queryOrderState(this.metadata.getLogname());

        List<PairConstraint> constraints = new ArrayList<>();
        List<PairConstraint> constraints0 = activityMatrix.filter(x->{
            return !x._1().equals(x._2());
        }).keyBy(x->{
            return new Tuple2<>(x._1(),x._2());
        }).subtractByKey(
            orderStateRDD.filter(y->{
                return y.getRule().contains("chain");
            })
            .distinct()
            .keyBy(x->{
                return new Tuple2<>(x.getEventA(),x.getEventB());
        })).map(x->{
            EventPairSupport eps = new EventPairSupport(x._1()._1(),x._1()._2(),1.0);
            return new PairConstraint(eps,"not-chain-succession");
        }).collect();
        constraints.addAll(constraints0);
        //get unique traces per event type
        JavaRDD<EventSupport> eventOccurrencesRDD = this.declareDBConnector.querySingleTable(this.metadata.getLogname());
        Map<String,Double> eventOccurrences = eventOccurrencesRDD.mapToPair(x->{
            return new Tuple2<>(x.getEvent(),x.getSupport());
        }).collectAsMap();
        Broadcast<Map<String,Double>> bEventOccurrences = javaSparkContext.broadcast(eventOccurrences);

        List<PairConstraint> constraints2 = orderStateRDD.flatMap((FlatMapFunction<OrderState,PairConstraint>)x->{
            List<PairConstraint> l = new ArrayList<>();
            double sup = x.getOccurrences()/bEventOccurrences.getValue().get(x.getEventB());
            if(x.getRule().contains("response")){
                sup = x.getOccurrences()/bEventOccurrences.getValue().get(x.getEventA());
            }
            l.add(new PairConstraint(new EventPairSupport(x.getEventA(),x.getEventB(),sup),x.getRule()));
            if(x.getRule().contains("chain")){
                l.add(new PairConstraint(new EventPairSupport(x.getEventA(),x.getEventB(),sup),"chain-succession"));
                l.add(new PairConstraint(new EventPairSupport(x.getEventA(),x.getEventB(),1-sup),"not-chain-succession"));
            }else if(x.getRule().contains("alternate")){
                l.add(new PairConstraint(new EventPairSupport(x.getEventA(),x.getEventB(),sup),"alternate-succession"));
            }else{
                l.add(new PairConstraint(new EventPairSupport(x.getEventA(),x.getEventB(),sup),"succession"));
                l.add(new PairConstraint(new EventPairSupport(x.getEventA(),x.getEventB(),1-sup),"not-succession"));
            }
            return l.iterator();
        })
        .keyBy(x->{
            return new Tuple3<>(x.getRule(),x.getEventPairSupport().getEventA(),x.getEventPairSupport().getEventB());
        })
        .reduceByKey((x,y)->{
            EventPairSupport eps = x.getEventPairSupport();
            eps.setEventA(x.getEventPairSupport().getEventA());
            eps.setEventB(x.getEventPairSupport().getEventB());
            eps.setSupport(x.getEventPairSupport().getSupport()*y.getEventPairSupport().getSupport());
            return new PairConstraint(eps,x.getRule());
        })
        .filter(x->{
            return x._2().getEventPairSupport().getSupport()>=bSupport.getValue();
        })
        .map(x->{
            return x._2();
        })
        .collect();

        constraints.addAll(constraints2);

        //handle negatives
        JavaRDD<NegativeState> negativeStateRDD = this.declareDBConnector.queryNegativeState(this.metadata.getLogname());
        List<PairConstraint> constraints3 = negativeStateRDD
        .map(x->{
            EventPairSupport eps = new EventPairSupport(x.get_1(),x.get_2(),1.0);
            return new PairConstraint(eps,"not-succession");
        })
        .collect();
        constraints.addAll(constraints3);  
        //return all the constraints to be filtered
        return constraints;

    }

    private void setSpecificConstraints(QueryOrderRelationWrapper qow, List<PairConstraint> constraints,
        QueryResponseOrderedRelationsState response){
        response.setMode(qow.getMode());
        // Wrapper has 3 modes simple, alternate and chain. 
        // Based on these values this methods keeps the correct constraints.
        for(PairConstraint uc:constraints){
            if(qow.getMode().equals("simple")){
                if(uc.getRule().equals("response") && uc.getRule().equals(qow.getConstraint())){  
                    response.getResponse().add(uc.getEventPairSupport());
                }else if(uc.getRule().equals("precedence") && uc.getRule().equals(qow.getConstraint())){
                    response.getPrecedence().add(uc.getEventPairSupport());
                }else if(uc.getRule().equals("succession") && uc.getRule().equals(qow.getConstraint())){
                    response.getSuccession().add(uc.getEventPairSupport());
                }else if(uc.getRule().equals("not-succession")){
                    response.getNotSuccession().add(uc.getEventPairSupport());
                }
            }else if(qow.getMode().equals("alternate")){
                if(uc.getRule().equals("alternate-succession") && qow.getConstraint().equals("alternate-succession")){
                    response.getSuccession().add(uc.getEventPairSupport());
                }else if(uc.getRule().equals("not-succession")){
                    response.getNotSuccession().add(uc.getEventPairSupport());
                }else if(uc.getRule().equals("alternate-response") && qow.getConstraint().equals("response")){
                    response.getResponse().add(uc.getEventPairSupport());
                }else if(uc.getRule().equals("alternate-precedence") && qow.getConstraint().equals("precedence")){
                    response.getPrecedence().add(uc.getEventPairSupport());
                }
            }
            else if(qow.getMode().equals("chain")){
                if(uc.getRule().equals("chain-succession") && qow.getConstraint().equals("succession")){
                    response.getSuccession().add(uc.getEventPairSupport());
                }else if(uc.getRule().equals("not-chain-succession")){
                    response.getNotSuccession().add(uc.getEventPairSupport());
                }else if(uc.getRule().equals("chain-response") && qow.getConstraint().equals("response")){
                    response.getResponse().add(uc.getEventPairSupport());
                }else if(uc.getRule().equals("chain-precedence") && qow.getConstraint().equals("precedence")){
                    response.getPrecedence().add(uc.getEventPairSupport());
                }
            }
        
        }
    }


}
