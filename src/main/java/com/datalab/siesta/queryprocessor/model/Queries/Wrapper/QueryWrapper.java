package com.datalab.siesta.queryprocessor.model.Queries.Wrapper;

import org.codehaus.jackson.annotate.JsonProperty;

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
