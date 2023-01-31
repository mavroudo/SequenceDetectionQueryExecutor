package com.datalab.siesta.queryprocessor.storage;

import com.datalab.siesta.queryprocessor.model.Metadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DBConnector {

    private DatabaseRepository db;

    @Autowired
    public DBConnector(DatabaseRepository databaseRepository){
        this.db=databaseRepository;
    }

    public Metadata getMetadata(String logname){
        return db.getMetadata(logname);
    }
}
