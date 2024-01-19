package com.datalab.siesta.queryprocessor.model.Serializations;

import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.GroupOccurrences;
import com.datalab.siesta.queryprocessor.model.Occurrence;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;

import java.io.IOException;

public class CustomGroupOccurrencesSerializer extends JsonSerializer<GroupOccurrences> {


    @Override
    public void serialize(GroupOccurrences value, org.codehaus.jackson.JsonGenerator gen, org.codehaus.jackson.map.SerializerProvider provider) throws IOException, JsonProcessingException {
        gen.writeStartObject();
        gen.writeNumberField("Group ID", value.getGroupId());

        gen.writeFieldName("occurrences");
        gen.writeStartArray();
        for (Occurrence occurrence : value.getOccurrences()) {
            gen.writeStartObject(); // Start of Occurrence object

            // Serialize each EventBoth in the Occurrence
            gen.writeArrayFieldStart("occurrence");
            for (EventBoth e : occurrence.getOccurrence()) {
                gen.writeStartObject(); // Start of EventBoth object
                gen.writeStringField("name", e.getName());
                gen.writeStringField("timestamp", e.getTimestamp().toString());
                gen.writeEndObject(); // End of EventBoth object
            }
            gen.writeEndArray(); // End of occurrence array

            gen.writeEndObject(); // End of Occurrence object
        }
        gen.writeEndArray();
        gen.writeEndObject();
    }
}

