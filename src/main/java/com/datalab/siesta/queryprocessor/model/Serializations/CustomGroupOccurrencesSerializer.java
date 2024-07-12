package com.datalab.siesta.queryprocessor.model.Serializations;

import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.GroupOccurrences;
import com.datalab.siesta.queryprocessor.model.Occurrence;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;


import java.io.IOException;

public class CustomGroupOccurrencesSerializer extends JsonSerializer<GroupOccurrences> {

    @Override
    public void serialize(GroupOccurrences groupOccurrences, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeNumberField("Group ID", groupOccurrences.getGroupId());

        jsonGenerator.writeFieldName("occurrences");
        jsonGenerator.writeStartArray();
        for (Occurrence occurrence : groupOccurrences.getOccurrences()) {
            jsonGenerator.writeStartObject(); // Start of Occurrence object

            // Serialize each EventBoth in the Occurrence
            jsonGenerator.writeArrayFieldStart("occurrence");
            for (EventBoth e : occurrence.getOccurrence()) {
                jsonGenerator.writeStartObject(); // Start of EventBoth object
                jsonGenerator.writeStringField("name", e.getName());
                jsonGenerator.writeStringField("timestamp", e.getTimestamp().toString());
                jsonGenerator.writeEndObject(); // End of EventBoth object
            }
            jsonGenerator.writeEndArray(); // End of occurrence array

            jsonGenerator.writeEndObject(); // End of Occurrence object
        }
        jsonGenerator.writeEndArray();
        jsonGenerator.writeEndObject();
    }
}

