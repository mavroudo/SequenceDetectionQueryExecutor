package com.fa.funnel.rest;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.web.servlet.ServletContextInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AbstractAsyncConfiguration;

/**
 *
 * @author Andreas Kosmatopoulos
 */
@Configuration
@EnableAutoConfiguration
public class ServletContextConfiguration extends AbstractAsyncConfiguration{

    /**
     * {@link ServletContextInitializer} implementation that enables data collection for 
     * Sentry's {@link io.sentry.event.helper.HttpEventBuilderHelper}.
     * @return the servlet context initializer implementation
     */
    @Bean
    public ServletContextInitializer sentryServletContextInitializer() {
        return new io.sentry.spring.SentryServletContextInitializer();
    }
}
