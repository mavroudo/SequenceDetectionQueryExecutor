package com.sequence.detection.rest;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.data.cassandra.config.CassandraClusterFactoryBean;
import org.springframework.data.cassandra.config.java.AbstractCassandraConfiguration;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

/**
 *
 * @author Andreas Kosmatopoulos
 */
@Configuration
@EnableAutoConfiguration
@PropertySource(value = { "classpath:cassandra.properties" })
public class CassandraConfiguration extends AbstractCassandraConfiguration
{

    @Autowired
    private Environment environment;

    /**
     * Creates an instance of the cluster upon which Cassandra is located
     * @return a cluster instance with the appropriate environment variables loaded
     */
    @Bean
    @Override
    public CassandraClusterFactoryBean cluster()
    {
        SSLContext sslContext = null;
        {
            KeyStore trustStore = null;
            try {
                trustStore = KeyStore.getInstance("JKS");
            } catch (KeyStoreException e) {
                e.printStackTrace();
            }
            try (final InputStream stream = Files.newInputStream(Paths.get(environment.getProperty("cassandra_connection_ssl_truststore_path")))) {

                assert trustStore != null;
                trustStore.load(stream, environment.getProperty("cassandra_connection_ssl_truststore_password").toCharArray());

            } catch (IOException | CertificateException | NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            final TrustManagerFactory trustManagerFactory;
            try {
                trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                trustManagerFactory.init(trustStore);

                sslContext = SSLContext.getInstance("TLS");
                sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
            } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
                e.printStackTrace();
            }
        }
        PoolingOptions poolOpts = new PoolingOptions();
        poolOpts.setMaxRequestsPerConnection(HostDistance.LOCAL,
                Integer.parseInt(environment.getProperty("cassandra_max_requests_per_local_connection")));
        poolOpts.setMaxRequestsPerConnection(HostDistance.REMOTE,
                Integer.parseInt(environment.getProperty("cassandra_max_requests_per_remote_connection")));
        poolOpts.setConnectionsPerHost(HostDistance.LOCAL,
                Integer.parseInt(environment.getProperty("cassandra_connections_per_host")),
                Integer.parseInt(environment.getProperty("cassandra_connections_per_host")));
        poolOpts.setMaxQueueSize(Integer.parseInt(environment.getProperty("cassandra_max_queue_size")));

        SocketOptions sockOpts = new SocketOptions();
        sockOpts.setConnectTimeoutMillis(Integer.parseInt(environment.getProperty("cassandra_connection_timeout")));
        sockOpts.setReadTimeoutMillis(Integer.parseInt(environment.getProperty("cassandra_read_timeout")));


        CassandraClusterFactoryBean cluster = new CassandraClusterFactoryBean();
        cluster.setContactPoints(environment.getProperty("cassandra_contactpoints"));
        cluster.setPort(Integer.parseInt(environment.getProperty("cassandra_port")));
        cluster.setUsername(environment.getProperty("cassandra_user"));
        cluster.setPassword(environment.getProperty("cassandra_password"));

        String sslEnabled = environment.getProperty("cassandra_ssl_enabled");
        if (Boolean.parseBoolean(sslEnabled)) {
            cluster.setSslEnabled(true);
            cluster.setSslOptions(JdkSSLOptions.builder().withSSLContext(sslContext).build());
        }

        final String cassandra_region = environment.getProperty("cassandra_region");
        if(cassandra_region != null && cassandra_region.isEmpty()) {
            cluster.setLoadBalancingPolicy(DCAwareRoundRobinPolicy.builder().withLocalDc(cassandra_region).build());
        } else {
            cluster.setLoadBalancingPolicy(new RoundRobinPolicy());
        }
        cluster.setPoolingOptions(poolOpts);
        cluster.setSocketOptions(sockOpts);

        return cluster;
    }

    /**
     * Returns the environment variable for the keyspace name
     * @return the keyspace name
     */
    @Override
    protected String getKeyspaceName()
    {
        return environment.getProperty("cassandra_keyspace");
    }

    /**
     * Returns the default implementation of a MappingContext for Cassandra
     * @return the default implementation of a MappingContext
     */
    @Bean
    @Override
    public CassandraMappingContext cassandraMapping() {
        return new BasicCassandraMappingContext();
    }


}