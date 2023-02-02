package com.datalab.siesta.queryprocessor.storage.repositories.CassandraRdd;

//import com.datastax.spark.connector.cql.CassandraConnector;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.stereotype.Service;

@Configuration
@PropertySource("classpath:application.properties")
@ConditionalOnProperty(
        value = "database",
        havingValue = "cassandra-rdd",
        matchIfMissing = true
)
@Service
public class SparkConfiguration {

    @Value("${app.name:siesta2}")
    private String appName;
    @Value("${master.uri:local[*]}")
    private String masterUri;
    @Value("${spring.data.cassandra.contact-points:rabbit.csd.auth.gr}")
    private String cassandra_host;
    @Value("${spring.data.cassandra.port:9042}")
    private String cassandra_port;
    @Value("${spring.data.cassandra.username:cassandra}")
    private String cassandra_user;
    @Value("${spring.data.cassandra.password:cassandra}")
    private String cassandra_pass;
    @Value("${spring.data.cassandra.keyspace-name:siesta}")
    private String cassandra_keyspace_name;

//    @Bean
//    public SparkConf sparkConf() {
//        return new SparkConf()
//                .setAppName(appName)
//                .setMaster(masterUri)
//                .set("spark.cassandra.connection.host", cassandra_host)
//                .set("spark.cassandra.auth.username", cassandra_user)
//                .set("spark.cassandra.auth.password", cassandra_pass)
//                .set("spark.cassandra.connection.port", cassandra_port);
//
//
//    }

    @Bean
    public JavaSparkContext javaSparkContext() {
        return new JavaSparkContext(this.sparkSession().sparkContext());
    }

//    @Bean
//    public CassandraConnector getConnector(){return CassandraConnector.apply(sparkConf());}

    @Bean
    public SparkSession sparkSession() {
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .config("spark.cassandra.connection.host", cassandra_host)
                .config("spark.cassandra.connection.port", cassandra_port)
                .config("spark.cassandra.auth.username", cassandra_user)
                .config("spark.cassandra.auth.password", cassandra_pass)
                .master(masterUri)
                .getOrCreate();


//        SparkSession spark = SparkSession
//                .builder()
//                .sparkContext(this.javaSparkContext().sc())
//                .appName(appName)
//                .getOrCreate();
        return spark;
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    public String getCassandra_host() {
        return cassandra_host;
    }

    public String getCassandra_port() {
        return cassandra_port;
    }

    public String getCassandra_user() {
        return cassandra_user;
    }

    public String getCassandra_pass() {
        return cassandra_pass;
    }

    public String getCassandra_keyspace_name() {
        return cassandra_keyspace_name;
    }
}