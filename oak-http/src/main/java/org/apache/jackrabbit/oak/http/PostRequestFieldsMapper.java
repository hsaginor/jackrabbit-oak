package org.apache.jackrabbit.oak.http;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class PostRequestFieldsMapper {

    private ByteArrayOutputStream buff = new ByteArrayOutputStream();
    private JsonGenerator jGenerator = null;
    
    PostRequestFieldsMapper() throws IOException {
        JsonFactory jfactory = new JsonFactory();
        jGenerator = jfactory
                .createGenerator(buff, JsonEncoding.UTF8);
        
        jGenerator.writeStartObject();
    }
    
    void writeField(String name, String value) throws IOException {
        
        try {
            jGenerator.writeNumberField(name, Long.parseLong(value));
            return;
        } catch(NumberFormatException e) {}
        
        try {
            jGenerator.writeNumberField(name, Double.parseDouble(value));
            return;
        } catch(NumberFormatException e) {}

        if("true".equals(value) || "false".equals(value)) {
            jGenerator.writeBooleanField(name, Boolean.parseBoolean(value));
            return;
        }
        
        jGenerator.writeObjectField(name, value);
    }
    
    JsonNode toJsonNode() throws IOException {
        jGenerator.close();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(new ByteArrayInputStream(buff.toByteArray()));
        return node;
    }
}
