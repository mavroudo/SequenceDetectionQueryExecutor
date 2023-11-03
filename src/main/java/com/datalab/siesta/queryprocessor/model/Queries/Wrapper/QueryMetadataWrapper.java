package com.datalab.siesta.queryprocessor.model.Queries.Wrapper;

/**
 * Metadata Query: it only requires the log database from which the user wants to retrieve the metadata
 */
public class QueryMetadataWrapper extends QueryWrapper {


    public QueryMetadataWrapper(){

    }

    @Override
    public String toString() {
        return "QueryMetadataWrapper{" +
                "log_name='" + log_name + '\'' +
                '}';
    }
}
