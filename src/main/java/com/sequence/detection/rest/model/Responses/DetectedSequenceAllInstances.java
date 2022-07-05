package com.sequence.detection.rest.model.Responses;

import com.sequence.detection.rest.model.Lifetime;

import java.util.List;
import java.util.Map;

public class DetectedSequenceAllInstances {


    private String query;

    private Map<String, List<Lifetime>> instances;

    public DetectedSequenceAllInstances(Map<String, List<Lifetime>> ids, String query) {
        this.instances = ids;
        this.query = query;
    }

    public Map<String, List<Lifetime>> getInstances() {
        return instances;
    }

    public void setInstances(Map<String, List<Lifetime>> instances) {
        this.instances = instances;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
