package com.sequence.detection.rest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 *
 * @author Andreas Kosmatopoulos
 */
@EnableScheduling
@EnableAutoConfiguration
@ComponentScan("com.sequence.detection.rest") // makes the need to put the class on root redundant
@SpringBootApplication
public class Application extends SpringBootServletInitializer
{
    /**
     * Load application properties from resources
     * @param application A reference to the Spring application
     * @return The Spring application with the proper resources
     */
    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application)
    {
        return application.sources(Application.class); // load application properties from resources
    }

    /**
     * The application entry point
     * @param args Application arguments
     */
    public static void main(String[] args)
    {
        SpringApplication.run(Application.class, args);
    }
}
