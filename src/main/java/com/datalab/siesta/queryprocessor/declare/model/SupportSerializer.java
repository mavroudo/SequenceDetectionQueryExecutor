package com.datalab.siesta.queryprocessor.declare.model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.text.DecimalFormat;
import java.io.IOException;

public class SupportSerializer extends JsonSerializer<Double> {

    private static final DecimalFormat df = new DecimalFormat("0.000");

    @Override
    public void serialize(Double aDouble, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeString(df.format(aDouble));
    }
}
