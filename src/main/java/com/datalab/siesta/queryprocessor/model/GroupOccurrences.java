package com.datalab.siesta.queryprocessor.model;


import com.datalab.siesta.queryprocessor.model.Serializations.CustomGroupOccurrencesSerializer;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.List;

/**
 * Extends the Occurrences object. Instead of defining the trace id that the list of occurrences is referring to, it
 * defines the group id that the occurrences refer to. The trace id it was set to -1 and due to the
 * GroupOccurrencesSerialization.class it is not a member of the response json
 */
@JsonSerialize(using = CustomGroupOccurrencesSerializer.class)
public class GroupOccurrences extends Occurrences{

    @JsonProperty("Group ID")
    private int groupId;

    public GroupOccurrences() {
    }

    public GroupOccurrences(int groupId, List<Occurrence> occurrences) {
        this.groupId = groupId;
        this.occurrences=occurrences;
        traceID=-1;
    }

    public int getGroupId() {
        return groupId;
    }

    public void setGroupId(int groupId) {
        this.groupId = groupId;
    }
}
