package com.datalab.siesta.queryprocessor.storage.repositories.CassandraRdd;

import com.datalab.siesta.queryprocessor.model.Metadata;
import com.datalab.siesta.queryprocessor.storage.DatabaseRepository;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import scala.Function1;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;



@Configuration
@ConditionalOnProperty(
        value = "database",
        havingValue = "cassandra-rdd",
        matchIfMissing = true
)
@ComponentScan
public class CassConnector implements DatabaseRepository {

    private SparkSession sparkSession;

    private JavaSparkContext javaSparkContext;



    @Autowired
    public CassConnector(SparkSession sparkSession, JavaSparkContext javaSparkContext){
        this.sparkSession=sparkSession;
        this.javaSparkContext=javaSparkContext;
    }

    @Override
    public Metadata getMetadata(String logname) {
        Dataset<Row> df = sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", "bpi_2017_meta", "keyspace", "siesta"))
                .load();
        Map<String,String> m = new HashMap<>();
        df.toJavaRDD().map((Function<Row, Tuple2<String, String>>) row ->
                new Tuple2<>(row.getString(0),row.getString(1) ))
                        .collect().forEach(t->{
                    m.put(t._1,t._2);
                });
        return new Metadata(m);
    }
}
