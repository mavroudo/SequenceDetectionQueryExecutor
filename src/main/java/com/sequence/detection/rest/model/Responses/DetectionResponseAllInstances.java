package com.sequence.detection.rest.model.Responses;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

public class DetectionResponseAllInstances {

    @JsonProperty("sub-queries found in sequences")
    private List<DetectedSequenceAllInstances> ids;

    public List<DetectedSequenceAllInstances> getIds() {
        return ids;
    }

    public void setIds(List<DetectedSequenceAllInstances> ids) {
        this.ids = ids;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DetectionResponseAllInstances that = (DetectionResponseAllInstances) o;
        return ids.equals(that.ids);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ids);
    }
}
