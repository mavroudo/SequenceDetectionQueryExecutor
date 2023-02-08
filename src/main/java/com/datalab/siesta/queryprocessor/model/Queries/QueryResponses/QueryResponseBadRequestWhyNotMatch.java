package com.datalab.siesta.queryprocessor.model.Queries.QueryResponses;

import org.codehaus.jackson.annotate.JsonProperty;

public class QueryResponseBadRequestWhyNotMatch extends QueryResponseBadRequestForDetection{

    @JsonProperty("Can it be transform to simple pattern")
    private boolean isSimple;

    public QueryResponseBadRequestWhyNotMatch() {
        super();
    }



    public boolean isSimple() {
        return isSimple;
    }

    public void setSimple(boolean simple) {
        isSimple = simple;
    }
}
