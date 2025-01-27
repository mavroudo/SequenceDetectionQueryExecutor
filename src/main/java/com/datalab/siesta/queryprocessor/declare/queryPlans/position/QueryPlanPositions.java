package com.datalab.siesta.queryprocessor.declare.queryPlans.position;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponsePosition;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryPositionWrapper;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.DBModel.Trace;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import scala.Tuple2;

@Component
@RequestScope
public class QueryPlanPositions implements QueryPlan{

    /**
     * Connection with the database
     */
    protected final DeclareDBConnector declareDBConnector;

    /**
     * Connection with the spark 
     */
    protected JavaSparkContext javaSparkContext;

    /**
     * Log Database's metadata
     */
    protected Metadata metadata;


    @Autowired
    public QueryPlanPositions(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext){
        this.declareDBConnector = declareDBConnector;
        this.javaSparkContext = javaSparkContext;
    }

    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryPositionWrapper qpw = (QueryPositionWrapper) qw;
        //get number of total traces in database
        Long totalTraces = metadata.getTraces();
        //broadcast support and traces size
        Broadcast<Double> bSupport = this.javaSparkContext.broadcast(qpw.getSupport());
        Broadcast<Long> bTraces = this.javaSparkContext.broadcast(totalTraces);
        //get all sequences from the sequence table
        JavaRDD<Trace> traces = declareDBConnector.querySequenceTableDeclare(this.metadata.getLogname());
        
        JavaRDD<Tuple2<String, String>> events = traces.map(x -> new Tuple2<>(x.getEvents().get(0).getName(), x.getEvents().get(x.getEvents().size() - 1).getName()));
        events.persist(StorageLevel.MEMORY_AND_DISK());
        List<Tuple2<String, Double>> firsts = filterThem(events.map(x -> new Tuple2<>(x._1, 1)),
                bSupport,bTraces);
        List<Tuple2<String, Double>> lasts = filterThem(events.map(x -> new Tuple2<>(x._2, 1)),
                bSupport,bTraces);
        
        //set up the response
        QueryResponsePosition response = new QueryResponsePosition();
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

    private List<Tuple2<String, Double>> filterThem(JavaRDD<Tuple2<String, Integer>> traces, Broadcast<Double> bSupport, Broadcast<Long> bTraces) {
        JavaPairRDD<String, Tuple2<String, Integer>> one = traces.keyBy(x -> x._1)
                .reduceByKey((x, y) -> new Tuple2<>(x._1, x._2 + y._2));
        JavaRDD<Tuple2<String, Double>> two = one.map(x -> new Tuple2<>(x._1, x._2._2.doubleValue() / bTraces.getValue()));
        //filter counts based on the support
        return two.filter(x -> x._2 >= bSupport.getValue()).collect();
    }

    @Override
    public void setMetadata(Metadata metadata) {
        this.metadata= metadata;
    }

}
