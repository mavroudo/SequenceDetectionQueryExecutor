package com.datalab.siesta.queryprocessor.declare;

import com.datalab.siesta.queryprocessor.model.DBModel.Trace;
import com.datalab.siesta.queryprocessor.storage.DatabaseRepository;

import org.apache.spark.api.java.JavaRDD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DeclareDBConnector {

    private DatabaseRepository db;

    @Autowired
    public DeclareDBConnector(DatabaseRepository databaseRepository){
        this.db=databaseRepository;
    }

    public JavaRDD<Trace> querySequenceTable(String logName){
        return db.querySequenceTable(logName);
    }

}
