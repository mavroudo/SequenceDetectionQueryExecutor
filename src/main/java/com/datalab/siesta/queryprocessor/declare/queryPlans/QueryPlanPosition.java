package com.datalab.siesta.queryprocessor.declare.queryPlans;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponsePosition;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.DBModel.Trace;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.List;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueryPlanPosition {

    protected DeclareDBConnector dbConnector;
    protected JavaSparkContext javaSparkContext;
    protected Metadata metadata;
    protected List<Tuple2<String, Double>> results;

    public QueryPlanPosition() {
    }

    @Autowired
    public QueryPlanPosition(DeclareDBConnector dbConnector, JavaSparkContext javaSparkContext) {
        this.dbConnector = dbConnector;
        this.javaSparkContext = javaSparkContext;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public QueryResponse execute(String log_database, double support) {
        //get number of total traces in database
        Long totalTraces = metadata.getTraces();
        //broadcast support and traces size
        Broadcast<Double> bSupport = this.javaSparkContext.broadcast(support);
        Broadcast<Long> bTraces = this.javaSparkContext.broadcast(totalTraces);
        //get all sequences from the sequence table
        JavaRDD<Trace> traces = dbConnector.querySequenceTable(log_database);
        //filter their first event and count them
        JavaRDD<Tuple2<String, Integer>> events = getTracesInPosition(traces);
        results = filterThem(events, bSupport, bTraces);
        return new QueryResponsePosition();
    }

    protected JavaRDD<Tuple2<String, Integer>> getTracesInPosition(JavaRDD<Trace> traces) {
        return null;
    }

    protected List<Tuple2<String, Double>> filterThem(JavaRDD<Tuple2<String, Integer>> traces, Broadcast<Double> bSupport, Broadcast<Long> bTraces) {
        JavaPairRDD<String, Tuple2<String, Integer>> one = traces.keyBy(x -> x._1)
                .reduceByKey((x, y) -> new Tuple2<>(x._1, x._2 + y._2));
        JavaRDD<Tuple2<String, Double>> two = one.map(x -> new Tuple2<>(x._1, x._2._2.doubleValue() / bTraces.getValue()));
        //filter counts based on the support
        return two.filter(x -> x._2 >= bSupport.getValue()).collect();

    }
}
