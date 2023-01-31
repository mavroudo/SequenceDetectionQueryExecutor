package com.datalab.siesta.queryprocessor.storage.repositories.S3;

import com.datalab.siesta.queryprocessor.model.Metadata;
import com.datalab.siesta.queryprocessor.storage.DatabaseRepository;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import org.apache.spark.sql.Dataset;

@Configuration
@ConditionalOnProperty(
        value = "database",
        havingValue = "s3",
        matchIfMissing = true
)
@Service
public class S3Connector implements DatabaseRepository {

    protected SparkSession sparkSession;

    protected JavaSparkContext javaSparkContext;

    private String bucket="s3a://siesta/";

    @Autowired
    public S3Connector(SparkSession sparkSession, JavaSparkContext javaSparkContext){
        this.sparkSession=sparkSession;
        this.javaSparkContext=javaSparkContext;
    }


    @Override
    public Metadata getMetadata(String logname) {
        Dataset<Row> df = sparkSession.read().json(String.format("%s%s%s",bucket,logname,"/meta.parquet/"));
        return new Metadata(df.collectAsList().get(0));
    }
}
