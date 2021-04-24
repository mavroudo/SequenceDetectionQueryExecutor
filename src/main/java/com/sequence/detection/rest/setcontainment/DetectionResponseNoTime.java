package com.sequence.detection.rest.setcontainment;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sequence.detection.rest.model.DetectedSequence;
import com.sequence.detection.rest.model.DetectionResponse;

import java.util.List;
import java.util.Objects;

public class DetectionResponseNoTime {

    @JsonProperty("sub-queries found in sequences")
    private List<DetectedSequenceNoTime> ids;

    public List<DetectedSequenceNoTime> getIds() {
        return ids;
    }

    public void setIds(List<DetectedSequenceNoTime> ids) {
        this.ids = ids;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DetectionResponseNoTime that = (DetectionResponseNoTime) o;
        return ids.equals(that.ids);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ids);
    }
}
