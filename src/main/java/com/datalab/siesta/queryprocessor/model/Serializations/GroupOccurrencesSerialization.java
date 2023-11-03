package com.datalab.siesta.queryprocessor.model.Serializations;

import com.datalab.siesta.queryprocessor.model.GroupOccurrences;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

import java.io.IOException;
import java.text.SimpleDateFormat;

/**
 * Custom serializer for the pattern matches found in the group of traces
 */
public class GroupOccurrencesSerialization extends JsonSerializer<GroupOccurrences> {

    @Override
    public void serialize(GroupOccurrences groupOccurrences, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS");
        jsonGenerator.writeStartObject();
        jsonGenerator.writeNumberField("Group ID",groupOccurrences.getGroupId());
        jsonGenerator.writeObject(groupOccurrences.getOccurrences());
        jsonGenerator.writeEndObject();

    }
}
