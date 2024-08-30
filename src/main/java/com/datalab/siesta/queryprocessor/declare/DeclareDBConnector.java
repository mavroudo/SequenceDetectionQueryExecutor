package com.datalab.siesta.queryprocessor.declare;

import com.datalab.siesta.queryprocessor.declare.model.EventPairToTrace;
import com.datalab.siesta.queryprocessor.declare.model.OccurrencesPerTrace;
import com.datalab.siesta.queryprocessor.declare.model.UniqueTracesPerEventPair;
import com.datalab.siesta.queryprocessor.declare.model.UniqueTracesPerEventType;
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


}
