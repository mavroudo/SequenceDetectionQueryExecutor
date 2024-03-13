package com.datalab.siesta.queryprocessor.declare;

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

    public JavaPairRDD<Tuple2<String,Long>, List<Integer>> querySingleTableAllDeclare(String logname){
        return this.db.querySingleTableAllDeclare(logname);
    }

    public JavaRDD<Tuple3<String,String,Long>> queryIndexOriginalDeclare(String logname){
        return this.db.queryIndexOriginalDeclare(logname);
    }


}
