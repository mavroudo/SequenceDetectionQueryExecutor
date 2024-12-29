package com.datalab.siesta.queryprocessor.declare.queryPlans.position;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.model.declareState.PositionState;
import com.datalab.siesta.queryprocessor.declare.queryPlans.QueryPlanState;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponsePositionState;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryPositionWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;

import scala.Tuple2;
import scala.Tuple3;

@Component
@RequestScope
public class QueryPlanPositionsState extends QueryPlanState {

    public QueryPlanPositionsState(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext){
        super(declareDBConnector,javaSparkContext);
    }


    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryPositionWrapper qpw = (QueryPositionWrapper) qw;
        

        Broadcast<Double> bSupport = this.javaSparkContext.broadcast(qpw.getSupport());
        Broadcast<Long> bTraces = this.javaSparkContext.broadcast(metadata.getTraces());

        QueryResponsePositionState response = this.extractConstraintsFunction(bSupport,bTraces,qpw);
        
        this.extractStatistics(qpw);
        response.setUpToDate(qpw.isStateUpToDate());
        if(!qpw.isStateUpToDate()){
            response.setEventsPercentage((qpw.getIndexedEvents()/metadata.getEvents())*100);
            response.setTracesPercentage((qpw.getIndexedTraces()/metadata.getTraces())*100);
            response.setMessage("State is not fully updated. Consider re-running the preprocess to get 100% accurate constraints");
        }else{
            response.setEventsPercentage(100);
            response.setTracesPercentage(100);
        }

        return response;
    }

    public QueryResponsePositionState extractConstraintsFunction(Broadcast<Double> bSupport, Broadcast<Long> bTraces, QueryPositionWrapper qpw){
        JavaRDD<Tuple3<String,String,Double>> data = this.declareDBConnector.queryPositionState(metadata.getLogname())
            .map((Function<PositionState,Tuple3<String,String,Double>>)x->{
                return new Tuple3<String,String,Double>(x.getRule(),x.getEvent_type(),x.getOccurrences()/bTraces.getValue());
            })
            .filter((Function<Tuple3<String,String,Double>,Boolean>)x->{
                return x._3() >= bSupport.getValue();
            });
            
        List<Tuple2<String, Double>> firsts = data.filter((Function<Tuple3<String,String,Double>,Boolean>)x->{
            return x._1().equals("first");
        }).map((Function<Tuple3<String,String,Double>,Tuple2<String,Double>>)x->{
            return new Tuple2<String,Double>(x._2(),x._3());
        }).collect();

        List<Tuple2<String, Double>> lasts = data.filter((Function<Tuple3<String,String,Double>,Boolean>)x->{
            return x._1().equals("last");
        }).map((Function<Tuple3<String,String,Double>,Tuple2<String,Double>>)x->{
            return new Tuple2<String,Double>(x._2(),x._3());
        }).collect();

        QueryResponsePositionState response = new QueryResponsePositionState();

        if(qpw.getMode().equals("first")){
            response.setFirstTuple(firsts);
        }else if(qpw.getMode().equals("last")){
            response.setLastTuple(lasts);
        }else{
            response.setFirstTuple(firsts);
            response.setLastTuple(lasts);
        }
        return response;
    }
    

}
