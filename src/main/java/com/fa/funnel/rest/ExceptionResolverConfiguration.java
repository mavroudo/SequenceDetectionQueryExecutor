package com.fa.funnel.rest;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AbstractAsyncConfiguration;
import org.springframework.web.servlet.HandlerExceptionResolver;

/**
 *
 * @author Andreas Kosmatopoulos
 */
@Configuration
@EnableAutoConfiguration
public class ExceptionResolverConfiguration extends AbstractAsyncConfiguration{

    /**
     * Records exceptions that a {@link org.springframework.web.servlet.mvc.Controller} throws to Sentry.
     * @return the appropriate exception handler resolver
     */
    @Bean
    public HandlerExceptionResolver sentryExceptionResolver() {
        return new io.sentry.spring.SentryExceptionResolver();
    }
}
