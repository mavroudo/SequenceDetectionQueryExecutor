package com.datalab.siesta.queryprocessor.declare.model;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;
import java.text.DecimalFormat;
import java.io.IOException;

public class SupportSerializer extends JsonSerializer<Double> {

    private static final DecimalFormat df = new DecimalFormat("0.000");

    @Override
    public void serialize(Double value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
        jgen.writeString(df.format(value));
    }

}
