package com.sequence.detection.rest.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A Step Name as provided by the JSON input
 *
 * @author Andreas Kosmatopoulos
 */
public class Name {
    @JsonProperty("activity_name")
    private String activityName;

    public void setActivityName(String logName) {
        this.activityName = logName;
    }

    public String getActivityName() {
        return this.activityName;
    }

    public Name() {
    }

    @Override
    public String toString() {
        return "{ activity_name: " + activityName + "}";
    }

    public static class Builder {
        private final Name name = new Name();

        public Builder activityName(String logName) {
            name.activityName = logName;
            return this;
        }

        public Name build() {
            return name;
        }
    }

}
