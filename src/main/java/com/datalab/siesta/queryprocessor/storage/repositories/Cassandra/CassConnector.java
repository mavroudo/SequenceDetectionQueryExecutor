package com.datalab.siesta.queryprocessor.storage.repositories.Cassandra;

import com.datalab.siesta.queryprocessor.model.Metadata;
import com.datalab.siesta.queryprocessor.storage.DatabaseRepository;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;


@Configuration
@ConditionalOnProperty(
        value = "database",
        havingValue = "cassandra",
        matchIfMissing = true
)
@Service
public class CassConnector implements DatabaseRepository {

    private SparkSession sparkSession;

    private JavaSparkContext javaSparkContext;

    private SparkConfiguration sc;

    private CassandraConnector cassandraConnector;

    @Autowired
    public CassConnector(SparkSession sparkSession, JavaSparkContext javaSparkContext, SparkConfiguration sc,
                         CassandraConnector cassandraConnector){
        this.sparkSession=sparkSession;
        this.javaSparkContext=javaSparkContext;
        this.sc=sc;
        this.cassandraConnector=cassandraConnector;
    }

    @Override
    public Metadata getMetadata(String logname) {
        CassandraJavaRDD df = CassandraJavaUtil.javaFunctions(sparkSession.sparkContext())
                .cassandraTable("siesta","bpi_2017_meta");

        return null;
    }
}
