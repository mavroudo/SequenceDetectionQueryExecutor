package com.fa.funnel.rest.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

public class DetectionResponse {


    @JsonProperty("sub-queries found in sequences")
    private List<DetectedSequence> ids;

    public List<DetectedSequence> getIds() {
        return ids;
    }

    public void setIds(List<DetectedSequence> ids) {
        this.ids = ids;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DetectionResponse that = (DetectionResponse) o;
        return ids.equals(that.ids);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ids);
    }

}
