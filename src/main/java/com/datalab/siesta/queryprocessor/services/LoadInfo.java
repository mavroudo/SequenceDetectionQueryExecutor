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


/**
 * This class is running during initialization and loads information from the storage. Specifically, it loads the metadata
 * for all the log databases, and also for each log database loads the different event types.
 */
@Service
@ComponentScan
public class LoadInfo {

    private DBConnector dbConnector;


    @Autowired
    public LoadInfo(DBConnector dbConnector) {
        this.dbConnector = dbConnector;

    }

    @Bean
    public LoadedMetadata getAllMetadata() {
        Map<String, Metadata> m = new HashMap<>();
        for (String l : dbConnector.findAllLongNames()) {
            Metadata metadata = dbConnector.getMetadata(l);
            // TODO: determine a way to find the starting ts
            if(metadata.getStart_ts()==null){
                metadata.setStart_ts("");
            }
            m.put(l, metadata);
        }
        return new LoadedMetadata(m);
    }

    @Bean
    public LoadedEventTypes getAllEventTypes() {
        Map<String, List<String>> response = new HashMap<>();
        for (String l : dbConnector.findAllLongNames()) {
            response.put(l, dbConnector.getEventNames(l));
        }
        return new LoadedEventTypes(response);
    }
}
