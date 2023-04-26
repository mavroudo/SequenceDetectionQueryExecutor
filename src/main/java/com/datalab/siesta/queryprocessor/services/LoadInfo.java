package com.datalab.siesta.queryprocessor.services;

import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service
@ComponentScan
public class LoadInfo {

    private DBConnector dbConnector;


    @Autowired
    public LoadInfo(DBConnector dbConnector){
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

    @Bean
    public LoadedEventTypes getAllEventTypes(){
        Map<String, List<String>> response = new HashMap<>();
        for (String l : dbConnector.findAllLongNames()){
            response.put(l,dbConnector.getEventNames(l));
        }
        return new LoadedEventTypes(response);
    }
}
