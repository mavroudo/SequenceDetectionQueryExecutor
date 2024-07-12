package com.datalab.siesta.queryprocessor.model.Serializations;

import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;



import java.io.IOException;
import java.text.SimpleDateFormat;

/**
 * Custom Serializer for the EventBoth Class
 */
public class EventBothSerializer extends JsonSerializer<EventBoth> {

    @Override
    public void serialize(EventBoth eventBoth, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS");
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField("name",eventBoth.getName());
        if(eventBoth.getPosition()!=-1){
            jsonGenerator.writeNumberField("position",eventBoth.getPosition());
        }
        if(eventBoth.getTimestamp()!=null){
            jsonGenerator.writeStringField("timestamp", sdf.format(eventBoth.getTimestamp()));
        }
        jsonGenerator.writeEndObject();

    }
}
