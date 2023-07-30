package com.datalab.siesta.queryprocessor.declare.queryPlans;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponsePosition;
import com.datalab.siesta.queryprocessor.model.DBModel.Trace;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.List;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueryPlanBoth extends QueryPlanPosition {

    public QueryPlanBoth() {
        super();
    }

    @Autowired
    public QueryPlanBoth(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext) {
        super(declareDBConnector, javaSparkContext);
    }

    @Override
    public QueryResponse execute(String log_database, double support) {
        //get number of total traces in database
        Long totalTraces = metadata.getTraces();
        //broadcast support and traces size
        Broadcast<Double> bSupport = this.javaSparkContext.broadcast(support);
        Broadcast<Long> bTraces = this.javaSparkContext.broadcast(totalTraces);
        //get all sequences from the sequence table
        JavaRDD<Trace> traces = dbConnector.querySequenceTable(log_database);

        JavaRDD<Tuple2<String, String>> events = traces.map(x -> new Tuple2<>(x.getEvents().get(0).getName(), x.getEvents().get(x.getEvents().size() - 1).getName()));
        events.persist(StorageLevel.MEMORY_AND_DISK());
        QueryResponsePosition queryResponsePosition = new QueryResponsePosition();
        List<Tuple2<String, Double>> firsts = filterThem(events.map(x -> new Tuple2<>(x._1, 1)),
                bSupport,bTraces);
        queryResponsePosition.setFirstTuple(firsts);
        List<Tuple2<String, Double>> lasts = filterThem(events.map(x -> new Tuple2<>(x._2, 1)),
                bSupport,bTraces);
        queryResponsePosition.setLastTuple(lasts);
        events.unpersist();
        return queryResponsePosition;

    }



}
