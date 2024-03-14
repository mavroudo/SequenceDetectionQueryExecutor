package com.datalab.siesta.queryprocessor.declare.queryPlans;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.model.*;
import com.datalab.siesta.queryprocessor.declare.queryPlans.existence.QueryPlanExistences;
import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderedRelations;
import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderedRelationsAlternate;
import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderedRelationsChain;
import com.datalab.siesta.queryprocessor.declare.queryPlans.position.QueryPlanBoth;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseAll;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseExistence;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseOrderedRelations;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponsePosition;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import lombok.Setter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;
import scala.Tuple2;

import java.util.*;


@Component
@RequestScope
public class QueryPlanDeclareAll {

    private QueryPlanOrderedRelations queryPlanOrderedRelations;
    private QueryPlanOrderedRelationsChain queryPlanOrderedRelationsChain;
    private QueryPlanOrderedRelationsAlternate queryPlanOrderedRelationsAlternate;
    private QueryPlanExistences queryPlanExistences;
    private QueryPlanBoth queryPlanBoth;
    private JavaSparkContext javaSparkContext;
    private DeclareDBConnector declareDBConnector;
    @Setter
    private Metadata metadata;


    @Autowired
    public QueryPlanDeclareAll(QueryPlanOrderedRelations queryPlanOrderedRelations, QueryPlanOrderedRelationsChain
            queryPlanOrderedRelationsChain, QueryPlanOrderedRelationsAlternate queryPlanOrderedRelationsAlternate,
                               QueryPlanExistences queryPlanExistences, QueryPlanBoth queryPlanBoth,
                               JavaSparkContext javaSparkContext, DeclareDBConnector declareDBConnector) {
        this.queryPlanOrderedRelations = queryPlanOrderedRelations;
        this.queryPlanOrderedRelationsAlternate = queryPlanOrderedRelationsAlternate;
        this.queryPlanOrderedRelationsChain = queryPlanOrderedRelationsChain;
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
        JavaRDD<EventPairToNumberOfTrace> joined = this.queryPlanExistences.joinUnionTraces(uPairs);
        QueryResponseExistence queryResponseExistence = this.queryPlanExistences.runAll(groupTimes,
                support, joined, bSupport, bTotalTraces, bUniqueSingle, uEventType);
        uEventType.unpersist();

        //create joined table for order relations
        //load data from query table
        JavaRDD<EventPairToTrace> indexRDD = declareDBConnector.queryIndexOriginalDeclare(logname)
                .filter(x->!x.getEventA().equals(x.getEventB()));

        JavaPairRDD<Tuple2<String, Long>, List<Integer>> singleRDD = declareDBConnector.querySingleTableAllDeclare(logname);
        singleRDD.persist(StorageLevel.MEMORY_AND_DISK());


        this.queryPlanOrderedRelations.initQueryResponse();
        this.queryPlanOrderedRelations.setMetadata(metadata);
        //join records from single table with records from the index table
        JavaRDD<EventPairTraceOccurrences> joinedOrder = this.queryPlanOrderedRelations.joinTables(indexRDD, singleRDD);
        joinedOrder.persist(StorageLevel.MEMORY_AND_DISK());
        singleRDD.unpersist();
        // extract simple ordered
        JavaRDD<Abstract2OrderConstraint> cSimple = this.queryPlanOrderedRelations.evaluateConstraint(joinedOrder
                ,"succession");
        cSimple.persist(StorageLevel.MEMORY_AND_DISK());
        //extract additional information required for pruning
        Map<String, Long> uEventType2 = declareDBConnector.extractTotalOccurrencesPerEventType(logname);
        Broadcast<Map<String, Long>> bUEventTypes = javaSparkContext.broadcast(uEventType2);
        //extract no-succession constraints from event pairs that do not exist in the database log
        this.queryPlanOrderedRelations.extendNotSuccession(uEventType2,logname,cSimple);
        //filter based on support
        this.queryPlanOrderedRelations.filterBasedOnSupport(cSimple, bUEventTypes, support);
        //load extracted constraints to the response
        QueryResponseOrderedRelations qSimple = this.queryPlanOrderedRelations.getQueryResponseOrderedRelations();
        cSimple.unpersist();

        //similar process to extract alternate ordered
        this.queryPlanOrderedRelationsAlternate.initQueryResponse();
        this.queryPlanOrderedRelationsAlternate.setMetadata(metadata);
        JavaRDD<Abstract2OrderConstraint> cAlternate = this.queryPlanOrderedRelationsAlternate
                .evaluateConstraint(joinedOrder,"succession");
        this.queryPlanOrderedRelationsAlternate.filterBasedOnSupport(cAlternate, bUEventTypes, support);
        QueryResponseOrderedRelations qAlternate = this.queryPlanOrderedRelationsAlternate
                .getQueryResponseOrderedRelations();

        //similar process to extract chain ordered
        this.queryPlanOrderedRelationsChain.initQueryResponse();
        this.queryPlanOrderedRelationsChain.setMetadata(metadata);
        JavaRDD<Abstract2OrderConstraint> cChain = this.queryPlanOrderedRelationsChain
                .evaluateConstraint(joinedOrder,"succession");
        cChain.persist(StorageLevel.MEMORY_AND_DISK());
        this.queryPlanOrderedRelationsChain.extendNotSuccession(uEventType2,logname,cChain);
        this.queryPlanOrderedRelationsChain.filterBasedOnSupport(cChain,bUEventTypes,support);
        QueryResponseOrderedRelations qChain = this.queryPlanOrderedRelationsChain
                .getQueryResponseOrderedRelations();
        cChain.unpersist();
        joinedOrder.unpersist();
        //combine all responses
        QueryResponseAll queryResponseAll = new QueryResponseAll(queryResponseExistence, queryResponsePosition, qSimple,
                qAlternate, qChain);
        return queryResponseAll;
    }
}
