package com.datalab.siesta.queryprocessor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;


/**
 * Main Class to start the application
 */
@SpringBootApplication
public class Application {

//    private static final Logger logger = LoggerFactory.getLogger(Application.class);


    /**
     * Initializes the Logger and starts the application
     * @param args
     */
    public static void main(String[] args) {
        ApplicationContext applicationContext =SpringApplication.run(Application.class, args);
    }

}
