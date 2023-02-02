package com.datalab.siesta.queryprocessor.storage.repositories.S3;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.EventPair;
import com.datalab.siesta.queryprocessor.model.Metadata;
import com.datalab.siesta.queryprocessor.storage.DatabaseRepository;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import org.apache.spark.sql.Dataset;
import scala.Function1;
import scala.collection.TraversableOnce;
import scala.collection.mutable.WrappedArray;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

    private String bucket = "s3a://siesta/";

    @Autowired
    public S3Connector(SparkSession sparkSession, JavaSparkContext javaSparkContext) {
        this.sparkSession = sparkSession;
        this.javaSparkContext = javaSparkContext;
    }


    @Override
    public Metadata getMetadata(String logname) {
        Dataset<Row> df = sparkSession.read().json(String.format("%s%s%s", bucket, logname, "/meta.parquet/"));
        return new Metadata(df.collectAsList().get(0));
    }

    @Override
    public Set<String> findAllLongNames() {
        try {
            FileSystem fs = FileSystem.get(new URI(this.bucket), sparkSession.sparkContext().hadoopConfiguration());
            RemoteIterator<LocatedFileStatus> f = fs.listFiles(new Path(this.bucket), true);
            Pattern pattern = Pattern.compile(String.format("%s[^/]*/",this.bucket));
            Set<String> files = new HashSet<>();
            while (f.hasNext()) {
                LocatedFileStatus fin = f.next();
                Matcher matcher = pattern.matcher(fin.getPath().toString());
                if (matcher.find()) {
                    String logname = matcher.group(0).replace(this.bucket, "").replace("/", "");
                    files.add(logname);
                }
            }
            return files;

        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public Map<EventPair, Count> getCounts(String logname, Set<EventPair> pairs) {
        String path = String.format("%s%s%s", bucket, logname, "/count.parquet/");
        String firstFilter = pairs.stream().map(x->x.getEventA().getName()).collect(Collectors.toSet())
                        .stream().map(x-> String.format("eventA = '%s'",x)).collect(Collectors.joining(" "));
        JavaRDD<WrappedArray<Row>> df = sparkSession.read()
                .parquet(path)
                .where(firstFilter)
                .javaRDD()
                .map(row->{
                    String eventA = row.getString(1);
                    WrappedArray<Row> countRecords = row.getAs("times");
                    return countRecords;

                    //TODO: finish this logic
//                    return new WrappedCount(eventA, (List<CountRecords>) countRecords.toList());
                });
        System.out.println("hey");





//                        row->{
//                    String eventA = row.getString(1);
//                    WrappedArray<CountRecords> wrappedArray= row.getAs("times");
//                })









        return null;
    }


}
