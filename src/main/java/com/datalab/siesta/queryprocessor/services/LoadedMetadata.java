package com.datalab.siesta.queryprocessor.services;


import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

/**
 * Maintains information for all the metadata for each log databsae
 */
public class LoadedMetadata {

    private Map<String, Metadata> metadata;

    @Autowired
    private DBConnector dbConnector;


    public LoadedMetadata(Map<String, Metadata> metadata) {
        this.metadata = metadata;
    }

    public Map<String, Metadata> getMetadata() {
        return metadata;
    }

    public Metadata getMetadata(String logname){
        if(metadata.containsKey(logname)) {
            return metadata.get(logname);
        }else{
            if(dbConnector.findAllLongNames().contains(logname)){
                return  dbConnector.getMetadata(logname);
            }else{
                return null;
            }
        }
    }


}
