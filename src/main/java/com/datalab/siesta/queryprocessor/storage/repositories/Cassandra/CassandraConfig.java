package com.datalab.siesta.queryprocessor.storage.repositories.Cassandra;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.cassandra.config.AbstractCassandraConfiguration;
import org.springframework.data.cassandra.config.CqlSessionFactoryBean;
import org.springframework.data.cassandra.core.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

@Configuration
@EnableAutoConfiguration
@PropertySource("classpath:application.properties")
@ConditionalOnProperty(
        value = "database",
        havingValue = "cassandra",
        matchIfMissing = true
)
public class CassandraConfig extends AbstractCassandraConfiguration {

    @Value("${cassandra.host:localhost}")
    private String cassandra_host;
    @Value("${cassandra.port:9042}")
    private String cassandra_port;
    @Value("${cassandra.user:cassandra}")
    private String cassandra_user;
    @Value("${cassandra.pass:cassandra}")
    private String cassandra_pass;
    @Value("${cassandra.keyspace_name:siesta}")
    private String cassandra_keyspace_name;


    @Override
    protected String getKeyspaceName() {
        return "siesta";
    }

    @Bean
    @Primary
    public CqlSessionFactoryBean cluster() {
        CqlSessionFactoryBean cluster = super.cassandraSession();
        cluster.setContactPoints("rabbit.csd.auth.gr");
        cluster.setPort(9042);
        cluster.setUsername("cassandra");
        cluster.setPassword("cassandra");
        return cluster;
    }

    @Bean
    public CassandraMappingContext cassandraMapping()
            throws ClassNotFoundException {
        return new BasicCassandraMappingContext();
    }
}
