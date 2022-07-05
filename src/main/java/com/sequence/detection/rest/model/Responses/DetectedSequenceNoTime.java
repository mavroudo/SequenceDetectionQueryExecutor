package com.sequence.detection.rest.model.Responses;
import java.util.List;

public class DetectedSequenceNoTime {
    private List<Long> ids;
    private String query;

    public DetectedSequenceNoTime(List<Long> ids, String query) {
        this.ids = ids;
        this.query = query;
    }


    public List<Long> getId() {
        return ids;
    }

    public void setId(List<Long> ids) {
        this.ids = ids;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
