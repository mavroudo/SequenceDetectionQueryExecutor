package com.datalab.siesta.queryprocessor.model.Queries.Wrapper;


import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * An abstract class that describes the query patterns. It has only one field and that is the name of the log database,
 * since all queries are expecting to have at least the name of the log database that they are referring to.
 * This abstract class  is a part of the generic query response procedure that follows the steps below:
 * Endpoint (query wrapper) -> QueryTypes -> creates a QueryPlan -> the QueryPlan is executed -> a QueryResponse is returned
 */
public abstract class QueryWrapper {
    
    @JsonProperty("log_name")
    String log_name = "";

    public String getLog_name() {
        return log_name;
    }

    public void setLog_name(String log_name) {
        this.log_name = log_name;
    }
}
