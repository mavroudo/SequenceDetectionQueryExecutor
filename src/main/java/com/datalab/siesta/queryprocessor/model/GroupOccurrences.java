package com.datalab.siesta.queryprocessor.model;

import com.datalab.siesta.queryprocessor.model.Serializations.EventBothSerializer;
import com.datalab.siesta.queryprocessor.model.Serializations.GroupOccurrencesSerialization;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.List;

@JsonSerialize(using = GroupOccurrencesSerialization.class)
public class GroupOccurrences extends Occurrences{

    private int groupId;


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
