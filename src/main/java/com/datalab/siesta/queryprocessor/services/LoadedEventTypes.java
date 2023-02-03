package com.datalab.siesta.queryprocessor.services;

import java.util.List;
import java.util.Map;

public class LoadedEventTypes {

    Map<String, List<String>> eventTypes;

    public LoadedEventTypes() {
    }

    public LoadedEventTypes(Map<String, List<String>> eventTypes) {
        this.eventTypes = eventTypes;
    }

    public Map<String, List<String>> getEventTypes() {
        return eventTypes;
    }

    public void setEventTypes(Map<String, List<String>> eventTypes) {
        this.eventTypes = eventTypes;
    }
}
