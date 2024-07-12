package com.datalab.siesta.queryprocessor.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * This mapper is used in the endpoints to process Request Bodies in the form of json objects.
 * Due to different libraries that can do the same thing, this object is initialized here so
 * every class will know which object to use
 */
@Configuration
public class JacksonConfig {

    @Bean
    public ObjectMapper objectMapper(){
        return new ObjectMapper();
    }
}
