package com.datalab.siesta.queryprocessor.model.Queries.QueryResponses;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * It extends the QueryResponseBadRequestForDetection response, by adding one more field. Since at this version
 * the explainability is only available in simple patterns (patterns that only contain simple events, i.e. events with
 * "_" as symbol)
 */
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
