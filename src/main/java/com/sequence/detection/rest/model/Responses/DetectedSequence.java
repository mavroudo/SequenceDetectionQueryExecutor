package com.sequence.detection.rest.model.Responses;

import com.sequence.detection.rest.model.Lifetime;

import java.util.Map;

public class DetectedSequence {

    private Map<String, Lifetime> ids;
    private String query;

    public DetectedSequence(Map<String, Lifetime> ids, String query) {
        this.ids = ids;
        this.query = query;
    }


    public Map<String, Lifetime> getId() {
        return ids;
    }

    public void setId(Map<String, Lifetime> ids) {
        this.ids = ids;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
