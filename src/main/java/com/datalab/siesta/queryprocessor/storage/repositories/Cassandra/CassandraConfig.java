package com.datalab.siesta.queryprocessor.storage.repositories.Cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.data.cassandra.config.AbstractCassandraConfiguration;
import org.springframework.data.cassandra.config.CqlSessionFactoryBean;
import org.springframework.data.cassandra.config.SessionFactoryFactoryBean;
import org.springframework.data.cassandra.core.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

@Configuration

@ConditionalOnProperty(
        value = "database",
        havingValue = "cassandra",
        matchIfMissing = true
)
public class CassandraConfig extends AbstractCassandraConfiguration {

    @Override
    protected String getKeyspaceName() {
        return "siesta";
    }


    @Override
    public CqlSessionFactoryBean cassandraSession(){
        CqlSessionFactoryBean cluster = new CqlSessionFactoryBean();
        cluster.setContactPoints("rabbit.csd.auth.gr");
        cluster.setPort(9042);
        cluster.setUsername("cassandra");
        cluster.setPassword("cassandra");
        cluster.setLocalDatacenter("datacenter1");

        return cluster;
    }


//    @Bean
//    public CassandraMappingContext cassandraMapping()
//            throws ClassNotFoundException {
//        return new BasicCassandraMappingContext();
//    }
}
