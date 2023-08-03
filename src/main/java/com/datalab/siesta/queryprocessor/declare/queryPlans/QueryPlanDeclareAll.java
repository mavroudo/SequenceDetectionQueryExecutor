package com.datalab.siesta.queryprocessor.declare.queryPlans;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.model.UniqueTracesPerEventPair;
import com.datalab.siesta.queryprocessor.declare.model.UniqueTracesPerEventType;
import com.datalab.siesta.queryprocessor.declare.queryPlans.existence.QueryPlanExistences;
import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderedRelations;
import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderedRelationsAlternate;
import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderedRelationsChain;
import com.datalab.siesta.queryprocessor.declare.queryPlans.position.QueryPlanBoth;
import com.datalab.siesta.queryprocessor.declare.queryPlans.position.QueryPlanPosition;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseAll;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseExistence;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseOrderedRelations;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponsePosition;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexPair;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
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
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueryPlanDeclareAll {

    private QueryPlanOrderedRelations queryPlanOrderedRelations;
    private QueryPlanOrderedRelationsChain queryPlanOrderedRelationsChain;
    private QueryPlanOrderedRelationsAlternate queryPlanOrderedRelationsAlternate;
    private QueryPlanExistences queryPlanExistences;
    private QueryPlanBoth queryPlanBoth;
    private JavaSparkContext javaSparkContext;
    private DeclareDBConnector declareDBConnector;
    private Metadata metadata;

    @Autowired
    public QueryPlanDeclareAll(QueryPlanOrderedRelations queryPlanOrderedRelations, QueryPlanOrderedRelationsChain
            queryPlanOrderedRelationsChain, QueryPlanOrderedRelationsAlternate queryPlanOrderedRelationsAlternate,
                               QueryPlanExistences queryPlanExistences, QueryPlanBoth queryPlanBoth,
                               JavaSparkContext javaSparkContext, DeclareDBConnector declareDBConnector) {
        this.queryPlanOrderedRelations = queryPlanOrderedRelations;
        this.queryPlanOrderedRelationsChain = queryPlanOrderedRelationsChain;
        this.queryPlanOrderedRelationsAlternate = queryPlanOrderedRelationsAlternate;
        this.queryPlanExistences = queryPlanExistences;
        this.queryPlanBoth = queryPlanBoth;
        this.javaSparkContext = javaSparkContext;
        this.declareDBConnector = declareDBConnector;
    }

    public QueryResponseAll execute(String logname, double support) {
        //run positions
        this.queryPlanBoth.setMetadata(metadata);
        QueryResponsePosition queryResponsePosition = (QueryResponsePosition) this.queryPlanBoth.execute(logname, support);
        //run existences
        this.queryPlanExistences.setMetadata(metadata);
        this.queryPlanExistences.initResponse();
        Broadcast<Double> bSupport = javaSparkContext.broadcast(support);
        Broadcast<Long> bTotalTraces = javaSparkContext.broadcast(metadata.getTraces());

        //if existence, absence or exactly in modes
        JavaRDD<UniqueTracesPerEventType> uEventType = declareDBConnector.querySingleTableDeclare(logname);
        uEventType.persist(StorageLevel.MEMORY_AND_DISK());
        Map<String, HashMap<Integer, Long>> groupTimes = this.queryPlanExistences.createMapForSingle(uEventType);
        Map<String, Long> singleUnique = this.queryPlanExistences.extractUniqueTracesSingle(groupTimes);
        Broadcast<Map<String, Long>> bUniqueSingle = javaSparkContext.broadcast(singleUnique);

        JavaRDD<UniqueTracesPerEventPair> uPairs = declareDBConnector.queryIndexTableDeclare(logname);
        uPairs.persist(StorageLevel.MEMORY_AND_DISK());
        JavaRDD<Tuple3<String, String, Integer>> joined = this.queryPlanExistences.joinUnionTraces(uPairs);
        joined.persist(StorageLevel.MEMORY_AND_DISK());
        QueryResponseExistence queryResponseExistence = this.queryPlanExistences.runAll(groupTimes,
                support, joined, bSupport, bTotalTraces, bUniqueSingle, uEventType);

        joined.unpersist();
        uPairs.unpersist();
        uEventType.unpersist();

        JavaRDD<IndexPair> indexPairsRDD = declareDBConnector.queryIndexTableAllDeclare(logname);
        JavaRDD<Tuple5<String, String, Long, Set<Integer>, Set<Integer>>> joined2 = this.queryPlanOrderedRelations.
                joinTables(indexPairsRDD);
        joined2.persist(StorageLevel.MEMORY_AND_DISK());
        Map<String, Long> uEventType2 = declareDBConnector.querySingleTableDeclare(logname)
                .map(x -> {
                    long all = x.getOccurrences().stream().mapToLong(y -> y._2).sum();
                    return new Tuple2<>(x.getEventType(), all);
                }).keyBy(x -> x._1).mapValues(x -> x._2).collectAsMap();
        Broadcast<Map<String, Long>> bUEventTypes = javaSparkContext.broadcast(uEventType2);
        //execute simple
        this.queryPlanOrderedRelations.initQueryResponse();
        this.queryPlanOrderedRelations.setMetadata(metadata);
        JavaRDD<Tuple4<String, String, String, Integer>> cSimple = this.queryPlanOrderedRelations
                .evaluateConstraint(joined2, "succession");
        this.queryPlanOrderedRelations.filterBasedOnSupport(cSimple, bUEventTypes, support);
        QueryResponseOrderedRelations qSimple = this.queryPlanOrderedRelations.getQueryResponseOrderedRelations();
        //execute alternate
        this.queryPlanOrderedRelationsAlternate.initQueryResponse();
        this.queryPlanOrderedRelationsAlternate.setMetadata(metadata);
        JavaRDD<Tuple4<String, String, String, Integer>> cAlternate = this.queryPlanOrderedRelationsAlternate
                .evaluateConstraint(joined2, "succession");
        this.queryPlanOrderedRelationsAlternate.filterBasedOnSupport(cAlternate, bUEventTypes, support);
        QueryResponseOrderedRelations qAlternate = this.queryPlanOrderedRelationsAlternate
                .getQueryResponseOrderedRelations();
        //execute chain
        this.queryPlanOrderedRelationsChain.initQueryResponse();
        this.queryPlanOrderedRelationsChain.setMetadata(metadata);
        JavaRDD<Tuple4<String, String, String, Integer>> cChain = this.queryPlanOrderedRelationsChain
                .evaluateConstraint(joined2, "succession");
        this.queryPlanOrderedRelationsChain.filterBasedOnSupport(cChain, bUEventTypes, support);
        QueryResponseOrderedRelations qChain = this.queryPlanOrderedRelationsChain
                .getQueryResponseOrderedRelations();
        joined2.unpersist();
        //combine all responses
        QueryResponseAll queryResponseAll = new QueryResponseAll(queryResponseExistence, queryResponsePosition, qSimple,
                qAlternate, qChain);
        return queryResponseAll;

    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }
}
