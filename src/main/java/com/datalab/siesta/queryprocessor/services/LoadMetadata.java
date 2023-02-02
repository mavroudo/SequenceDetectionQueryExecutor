package com.datalab.siesta.queryprocessor.services;

import com.datalab.siesta.queryprocessor.model.LoadedMetadata;
import com.datalab.siesta.queryprocessor.model.Metadata;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;


@Service
@ComponentScan
public class LoadMetadata {

    private DBConnector dbConnector;


    @Autowired
    public LoadMetadata(DBConnector dbConnector){
        this.dbConnector=dbConnector;

    }

    @Bean
    public LoadedMetadata getAllMetadata(){
        Map<String,Metadata> m = new HashMap<>();
        for (String l : dbConnector.findAllLongNames()){
            m.put(l,dbConnector.getMetadata(l));
        }
        return new LoadedMetadata(m);
    }
}
