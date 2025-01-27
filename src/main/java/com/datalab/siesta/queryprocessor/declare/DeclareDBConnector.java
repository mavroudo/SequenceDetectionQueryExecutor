package com.datalab.siesta.queryprocessor.declare;

import com.datalab.siesta.queryprocessor.declare.model.EventPairToTrace;
import com.datalab.siesta.queryprocessor.declare.model.EventSupport;
import com.datalab.siesta.queryprocessor.declare.model.OccurrencesPerTrace;
import com.datalab.siesta.queryprocessor.declare.model.UniqueTracesPerEventPair;
import com.datalab.siesta.queryprocessor.declare.model.UniqueTracesPerEventType;
import com.datalab.siesta.queryprocessor.declare.model.declareState.ExistenceState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.NegativeState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.OrderState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.PositionState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.UnorderStateI;
import com.datalab.siesta.queryprocessor.declare.model.declareState.UnorderStateU;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexPair;
import com.datalab.siesta.queryprocessor.model.DBModel.Trace;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.storage.DatabaseRepository;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import scala.Tuple3;

import java.util.List;
import java.util.Map;

@Service
public class DeclareDBConnector {

    private DatabaseRepository db;

    @Autowired
    public DeclareDBConnector(DatabaseRepository databaseRepository){
        this.db=databaseRepository;
    }

    public JavaRDD<Trace> querySequenceTableDeclare(String logName){
        return db.querySequenceTableDeclare(logName);
    }

    public JavaRDD<UniqueTracesPerEventType> querySingleTableDeclare(String logname){
        return this.db.querySingleTableDeclare(logname);
    }

    public JavaRDD<EventSupport> querySingleTable(String logname){
        return this.db.querySingleTable(logname);
    }


    public JavaRDD<UniqueTracesPerEventPair> queryIndexTableDeclare(String logname){
        return this.db.queryIndexTableDeclare(logname);
    }

    public JavaRDD<IndexPair> queryIndexTableAllDeclare(String logname){
        return this.db.queryIndexTableAllDeclare(logname);
    }

    public JavaPairRDD<Tuple2<String,String>, List<Integer>> querySingleTableAllDeclare(String logname){
        return this.db.querySingleTableAllDeclare(logname);
    }


    public JavaRDD<EventPairToTrace> queryIndexOriginalDeclare(String logname){
        return this.db.queryIndexOriginalDeclare(logname);
    }

    public Map<String,Long> extractTotalOccurrencesPerEventType(String logname){
        return this.querySingleTableDeclare(logname)
                .map(x -> {
                    long all = x.getOccurrences().stream().mapToLong(OccurrencesPerTrace::getOccurrences).sum();
                    return new Tuple2<>(x.getEventType(), all);
                }).keyBy(x -> x._1).mapValues(x -> x._2).collectAsMap();
    }

    public JavaRDD<PositionState> queryPositionState(String logname){
        return this.db.queryPositionState(logname);
    }

    public JavaRDD<ExistenceState> queryExistenceState(String logname){
        return this.db.queryExistenceState(logname);
    }

    public JavaRDD<UnorderStateI> queryUnorderStateI(String logname){
        return this.db.queryUnorderStateI(logname);
    }
    public JavaRDD<UnorderStateU> queryUnorderStateU(String logname){
        return this.db.queryUnorderStateU(logname);
    }
    public JavaRDD<OrderState> queryOrderState(String logname){
        return this.db.queryOrderState(logname);
    }
    public JavaRDD<NegativeState> queryNegativeState(String logname){
        return this.db.queryNegativeState(logname);
    }




}
