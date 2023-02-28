package com.datalab.siesta.queryprocessor.Signatures;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.Map;


@Configuration
@PropertySource("classpath:application.properties")
@ConditionalOnProperty(
        value = "database",
        havingValue = "cassandra-rdd"
)
@Service
public class CassandraConnectionSignature {

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
    private SparkSession sparkSession;
    private JavaSparkContext javaSparkContext;

    @Autowired
    public CassandraConnectionSignature(SparkSession sparkSession, JavaSparkContext javaSparkContext) { //this can be changed to use SparkConfig latter
        this.sparkSession = sparkSession;
        this.javaSparkContext = javaSparkContext;
    }

    public Signature getSignature(String logname) {
        Dataset<Row> df = this.sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", logname + "_sign_meta", "keyspace", this.cassandra_keyspace_name))
                .load();


        return null;

    }


}
