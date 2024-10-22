package com.datalab.siesta.queryprocessor.storage.repositories.DeltaLakes;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

/**
 * Contains the configuration of spark in he.maven.plugins:maven-compiler-plugin:3.13.0:compile (default-compile) on project siesta-query-processor: Fatal error compiling: error: release version 17 not supported -> [Help 1]
 order to connect to s3 database
 */
@Configuration
@PropertySource("classpath:application.properties")
@ConditionalOnExpression("'${database}' == 's3' and '${delta}' == 'true'")
public class SparkConfiguration {

    @Value("${app.name:siesta2}")
    private String appName;

    @Value("${master.uri:local[*]}")
    private String masterUri;

    @Value("${s3.user:minioadmin}")
    private String s3user;

    @Value("${s3.key:minioadmin}")
    private String s3key;

    @Value("${s3.timeout:600000}")
    private String s3timeout;

    @Value("${s3.endpoint:http://127.0.0.1:9000}")
    private String s3endpoint;

    public SparkConf sparkConf() {
        return new SparkConf()
                .setAppName(appName)
                .setMaster(masterUri)
                .set("spark.driver.extraJavaOptions", "--add-opens java.base/sun.security.action=ALL-UNNAMED")
                .set("spark.executor.extraJavaOptions", "--add-opens java.base/sun.security.action=ALL-UNNAMED")
//                .set("spark.driver.memory","25g")
//                .set("spark.driver.memoryOverhead","2g")
//                .set("spark.memory.fraction","0.8")
//                .set("spark.memory.storageFraction","0.5")
                .set("spark.driver.maxResultSize","5g");
    }

    @Bean
    public JavaSparkContext javaSparkContext() {
        return new JavaSparkContext(this.sparkConf());
    }

    @Bean
    public SparkSession sparkSession() {
        SparkSession spark= SparkSession
                .builder()
                .sparkContext(this.javaSparkContext().sc())
                .appName("siesta 2")
                .getOrCreate();
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", s3endpoint);
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", s3user);
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", s3key);
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.connection.timeout", s3timeout);
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.path.style.access", "true");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.bucket.create.enabled", "true");
        spark.conf().set("spark.sql.sources.partitionOverwriteMode", "dynamic");
//        spark.conf().set("spark.executor.memory", "30g");
        return spark;
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }
}
