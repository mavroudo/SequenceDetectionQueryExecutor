package com.datalab.siesta.queryprocessor.services;


import com.datalab.siesta.queryprocessor.model.Metadata;

import java.util.Map;

public class LoadedMetadata {

    private Map<String, Metadata> metadata;


    public LoadedMetadata(Map<String, Metadata> metadata) {
        this.metadata = metadata;
    }

    public Map<String, Metadata> getMetadata() {
        return metadata;
    }


}
