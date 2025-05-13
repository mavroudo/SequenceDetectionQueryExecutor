package com.datalab.siesta.queryprocessor.storage.repositories.S3;

import com.datalab.siesta.queryprocessor.declare.model.EventPairToTrace;
import com.datalab.siesta.queryprocessor.declare.model.EventSupport;
import com.datalab.siesta.queryprocessor.declare.model.OccurrencesPerTrace;
import com.datalab.siesta.queryprocessor.declare.model.UniqueTracesPerEventPair;
import com.datalab.siesta.queryprocessor.declare.model.UniqueTracesPerEventType;
import com.datalab.siesta.queryprocessor.declare.model.declareState.ExistenceState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.NegativeState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.OrderState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.PositionState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.UnorderStateI;
import com.datalab.siesta.queryprocessor.declare.model.declareState.UnorderStateU;
import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexPair;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.DBModel.Trace;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.repositories.SparkDatabaseRepository;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Configuration
//@ConditionalOnProperty(
//        value = "database",
//        havingValue = "s3",
//        matchIfMissing = true
//)
@ConditionalOnExpression("'${database}' == 's3' and '${delta}' == 'false'")
@Service
public class S3Connector extends SparkDatabaseRepository {


    private String bucket = "s3a://siesta/";

    @Autowired
    public S3Connector(SparkSession sparkSession, JavaSparkContext javaSparkContext, Utils utils) {
        super(sparkSession, javaSparkContext, utils);
    }


    @Override
    public Metadata getMetadata(String logname) {
        Dataset<Row> df = sparkSession.read().parquet(String.format("%s%s%s", bucket, logname, "/meta.parquet/"));
        return new Metadata(df.toJavaRDD().collect().get(0));
    }

    @Override
    public Set<String> findAllLongNames() {
        try {
            FileSystem fs = FileSystem.get(new URI(this.bucket), sparkSession.sparkContext().hadoopConfiguration());
            RemoteIterator<LocatedFileStatus> f = fs.listFiles(new Path(this.bucket), true);
            Pattern pattern = Pattern.compile(String.format("%s[^/]*/", this.bucket));
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
    public List<Count> getCountForExploration(String logname, String event) {
        String path = String.format("%s%s%s", bucket, logname, "/count.parquet/");
        List<Count> counts = sparkSession.read()
                .parquet(path)
                .where(String.format("eventA = '%s'", event))
                .toJavaRDD()
                .flatMap((FlatMapFunction<Row, Count>) row -> {
                    String eventA = row.getString(1);
                    List<Row> countRecords = JavaConverters.seqAsJavaList(row.getSeq(0));
                    List<Count> c = new ArrayList<>();
                    for (Row v1 : countRecords) {
                        String eventB = v1.getString(0);
                        long sum_duration = v1.getLong(1);
                        int count = v1.getInt(2);
                        long min_duration = v1.getLong(3);
                        long max_duration = v1.getLong(4);
                        double sum_squared = v1.getDouble(5);
                        c.add(new Count(eventA, eventB, sum_duration, count, min_duration, max_duration, sum_squared));
                    }
                    return c.iterator();
                }).collect();
        return new ArrayList<>(counts);
    }

    @Override
    public List<Count> getCounts(String logname, Set<EventPair> pairs) {
        String path = String.format("%s%s%s", bucket, logname, "/count.parquet/");
        String firstFilter = pairs.stream().map(x -> x.getEventA().getName()).collect(Collectors.toSet())
                .stream().map(x -> String.format("eventA = '%s'", x)).collect(Collectors.joining(" or "));
        Broadcast<Set<EventPair>> b_pairs = javaSparkContext.broadcast(pairs);
        List<Count> counts = sparkSession.read()
                .parquet(path)
                .where(firstFilter)
                .toJavaRDD()
                .flatMap((FlatMapFunction<Row, Count>) row -> {
                    String eventA = row.getString(1);
                    List<Row> countRecords = JavaConverters.seqAsJavaList(row.getSeq(0));
                    List<Count> c = new ArrayList<>();
                    for (Row v1 : countRecords) {
                        String eventB = v1.getString(0);
                        long sum_duration = v1.getLong(1);
                        int count = v1.getInt(2);
                        long min_duration = v1.getLong(3);
                        long max_duration = v1.getLong(4);
                        double sum_squares = v1.getDouble(5);
                        c.add(new Count(eventA, eventB, sum_duration, count, min_duration, max_duration, sum_squares));
                    }
                    return c.iterator();
                })
                .filter((Function<Count, Boolean>) c -> {
                    for (EventPair p : b_pairs.getValue()) {
                        if (c.getEventA().equals(p.getEventA().getName()) && c.getEventB().equals(p.getEventB().getName())) {
                            return true;
                        }
                    }
                    return false;
                })
                .collect();
        List<Count> response = new ArrayList<>();
        pairs.forEach(p -> {
            for (Count c : counts) {
                if (c.getEventA().equals(p.getEventA().getName()) && c.getEventB().equals(p.getEventB().getName())) {
                    response.add(c);
                    break;
                }
            }
        });

        return response;
    }

    @Override
    public List<Count> getEventPairs(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/count.parquet/");
        List<Count> counts = sparkSession.read()
                .parquet(path)
                .toJavaRDD()
                .flatMap((FlatMapFunction<Row, Count>) row -> {
                    String eventA = row.getString(1);
                    List<Row> countRecords = JavaConverters.seqAsJavaList(row.getSeq(0));
                    List<Count> c = new ArrayList<>();
                    for (Row v1 : countRecords) {
                        String eventB = v1.getString(0);
                        long sum_duration = v1.getLong(1);
                        int count = v1.getInt(2);
                        long min_duration = v1.getLong(3);
                        long max_duration = v1.getLong(4);
                        double sum_squares = v1.getDouble(5);
                        c.add(new Count(eventA, eventB, sum_duration, count, min_duration, max_duration, sum_squares));
                    }
                    return c.iterator();
                })
                .collect();
        return counts;
    }

    @Override
    public List<String> getEventNames(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/single.parquet/");
        return sparkSession.read().parquet(path)
                .select("event_type")
                .distinct()
                .toJavaRDD()
                .map((Function<Row, String>) row -> row.getString(0))
                .collect();
    }


    @Override
    protected JavaRDD<Trace> querySequenceTablePrivate(String logname, Broadcast<Set<String>> bTraceIds) {
        return querySequenceTableDeclare(logname)
                .filter((Function<Trace, Boolean>) trace -> bTraceIds.getValue().contains(trace.getTraceID()));
    }


    @Override
    protected JavaRDD<EventBoth> getFromSingle(String logname, Set<String> traceIds, Set<String> eventTypes) {
        String path = String.format("%s%s%s", bucket, logname, "/single.parquet/");
        Broadcast<Set<String>> bTraceIds = javaSparkContext.broadcast(traceIds);
        Broadcast<Set<String>> bEventTypes = javaSparkContext.broadcast(eventTypes);
        return sparkSession.read()
                .parquet(path)
                .toJavaRDD()
                .filter((Function<Row, Boolean>) x -> bEventTypes.value().contains((String)x.getAs("event_type")))
                .filter((Function<Row, Boolean>) x -> bTraceIds.value().contains((String)x.getAs("trace_id")))
                .map((Function<Row, EventBoth>) row->{
                    String trace_id = row.getAs("trace_id");
                    String event_type = row.getAs("event_type");
                    String ts = row.getAs("timestamp");
                    Integer position = row.getAs("position");
                    return new EventBoth(event_type,trace_id,Timestamp.valueOf(ts),position);
                });
    }


    @Override
    protected JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>> getAllEventPairs(Set<EventPair> pairs,
                                                                                                  String logname,
                                                                                                  Metadata metadata,
                                                                                                  Timestamp from,
                                                                                                  Timestamp till) {
        String path = String.format("%s%s%s", bucket, logname, "/index.parquet/");
        Broadcast<Set<EventPair>> bPairs = javaSparkContext.broadcast(pairs);
        Broadcast<String> mode = javaSparkContext.broadcast(metadata.getMode());
        Broadcast<Timestamp> bFrom = javaSparkContext.broadcast(from);
        Broadcast<Timestamp> bTill = javaSparkContext.broadcast(till);

        List<String> whereStatements = new ArrayList<>();
        whereStatements.add(
                pairs.stream().map(x -> x.getEventA().getName()).distinct()
                        .map(p -> String.format("eventA = '%s'", p))
                        .collect(Collectors.joining(" or ")));

        for (int i = 0; i < whereStatements.size(); i++) {
            whereStatements.set(i, String.format("( %s )", whereStatements.get(i)));
        }
        String whereStatement = String.join(" and ", whereStatements);

        JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>> rows = sparkSession.read()
                .parquet(path)
                .where(whereStatement)
                .toJavaRDD()
                .flatMap((FlatMapFunction<Row, IndexPair>) row -> {
                    String eventA = row.getAs("eventA");
                    String eventB = row.getAs("eventB");
                    boolean checkContained = false;
                    for (EventPair ep : bPairs.getValue()) {
                        if (eventA.equals(ep.getEventA().getName()) && eventB.equals(ep.getEventB().getName())) {
                            checkContained = true;
                            break;
                        }
                    }
                    List<IndexPair> response = new ArrayList<>();
                    if (checkContained) {
                        String tid = row.getAs("trace_id");
                        if (mode.getValue().equals("positions")) {
                            int posA = row.getAs("positionA");
                            int posB = row.getAs("positionB");
                            response.add(new IndexPair(tid, eventA, eventB, posA, posB));
                        } else {
                            Timestamp tsA = row.getAs("timestampA");
                            Timestamp tsB = row.getAs("timestampB");
                            if (!(bTill.value() != null && tsA.after(bTill.value()) ||
                                    bFrom.value() != null && tsB.before(bFrom.value()))) {
                                response.add(new IndexPair(tid, eventA, eventB, tsA, tsB));
                            }
                        }
                    }
                    return response.iterator();
                })
                .groupBy((Function<IndexPair, Tuple2<String, String>>) indexPair -> new Tuple2<>(indexPair.getEventA(), indexPair.getEventB()));
        return rows;
    }


    //Below are for declare//

    @Override
    public JavaRDD<Trace> querySequenceTableDeclare(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/seq.parquet/");
        return sparkSession.read()
                .parquet(path)
                .toJavaRDD()
                .map((Function<Row, EventBoth>) row -> {
                    String trace_id = row.getAs("trace_id");
                    String event_name = row.getAs("event_type");
                    Timestamp ts = Timestamp.valueOf((String) row.getAs("timestamp"));
                    Integer pos = row.getAs("position");
                    return new EventBoth(event_name, trace_id, ts, pos);
                })
                .groupBy((Function<EventBoth, String>) EventBoth::getTraceID)
                .map((Function<Tuple2<String, Iterable<EventBoth>>, Trace>) t ->
                        new Trace(t._1(), IteratorUtils.toList(t._2().iterator()))
                );
    }

    @Override
    public JavaRDD<UniqueTracesPerEventType> querySingleTableDeclare(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/single.parquet/");

        return sparkSession.read()
                .parquet(path)
                .select("event_type","trace_id")
                .groupBy("event_type","trace_id")
                .agg(functions.size(functions.collect_list("event_type")).alias("unique"))
                .toJavaRDD()
                .groupBy((Function<Row, String>) ev->ev.getAs("event_type"))
                .map((Function<Tuple2<String, Iterable<Row>>,UniqueTracesPerEventType>) ev->{
                    String event_type = ev._1();
                    List<OccurrencesPerTrace> opt = new ArrayList<>();
                    for(Row r: ev._2()){
                        opt.add(new OccurrencesPerTrace(r.getAs("trace_id"),r.getAs("unique")));
                    }
                    return new UniqueTracesPerEventType(event_type,opt);
                });
    }

    @Override
    public JavaRDD<EventSupport> querySingleTable(String logname){
        String path = String.format("%s%s%s", bucket, logname, "/single.parquet/");

        return sparkSession.read()
                .parquet(path)
                .select("event_type","trace_id")
                .groupBy("event_type")
                .agg(functions.size(functions.collect_list("event_type")).alias("unique"))
                .toJavaRDD()
                .map((Function<Row, EventSupport>) row -> {
                    String event = row.getAs("event_type");
                    int s = row.getAs("unique");
                    return new EventSupport(event,s);
                });
    }

    @Override
    public JavaPairRDD<Tuple2<String, String>, List<Integer>> querySingleTableAllDeclare(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/single.parquet/");
        JavaPairRDD<Tuple2<String, String>, List<Integer>> rdd = sparkSession.read()
                .parquet(path)
                .select("event_type","trace_id","position")
                .groupBy("event_type","trace_id")
                .agg(functions.collect_list("position").alias("positions"))
                .toJavaRDD()
                .map(row->{
                    String eventType = row.getAs("event_type");
                    String trace_id = row.getAs("trace_id");
                    List<Integer> positions = JavaConverters.seqAsJavaList(row.getSeq(2));
                    return new Tuple3<>(eventType,trace_id,positions);
                })
                .keyBy(r -> new Tuple2<>(r._1(), r._2()))
                .mapValues(Tuple3::_3);

        return rdd;

    }

    @Override
    public JavaRDD<EventPairToTrace> queryIndexOriginalDeclare(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/index.parquet/");

        return sparkSession.read()
                .parquet(path)
                .select("eventA","eventB","trace_id")
                .distinct()
                .as(Encoders.bean(EventPairToTrace.class))
                .toJavaRDD();
    }

    @Override
    public JavaRDD<UniqueTracesPerEventPair> queryIndexTableDeclare(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/index.parquet/");

        return sparkSession.read().parquet(path)
                .select("eventA","eventB","trace_id")
                .distinct()
                .toJavaRDD()
                .groupBy((Function<Row, Tuple2<String,String>>)row->new Tuple2<>(row.getAs("eventA"),row.getAs("eventB")))
                .map((Function<Tuple2<Tuple2<String,String>, Iterable<Row>>, UniqueTracesPerEventPair>)row->{
                    List<String> uniqueTraces = new ArrayList<>();
                    for(Row r: row._2()){
                        uniqueTraces.add(r.getAs("trace_id"));
                    }
                    return new UniqueTracesPerEventPair(row._1()._1(),row._1()._2,uniqueTraces);
                } );
    }

    @Override
    public JavaRDD<IndexPair> queryIndexTableAllDeclare(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/index.parquet/");

        return sparkSession.read()
                .parquet(path)
                .toJavaRDD()
                .map((Function<Row, IndexPair>) row -> {
                    String eventA = row.getAs("eventA");
                    String eventB = row.getAs("eventB");
                    String trace_id = row.getAs("trace_id");
                    int positionA = row.getAs("positionA");
                    int positionB = row.getAs("positionB");
                    return new IndexPair(trace_id,eventA,eventB,positionA,positionB);
                });
    }


    @Override
    public JavaRDD<PositionState> queryPositionState(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/declare/position.parquet/");

        return sparkSession.read()
        .parquet(path)
        .as(Encoders.bean(PositionState.class))
        .toJavaRDD();
    }

    @Override
    public JavaRDD<ExistenceState> queryExistenceState(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/declare/existence.parquet/");

        return sparkSession.read()
        .parquet(path)
        .as(Encoders.bean(ExistenceState.class))
        .toJavaRDD();
    }


    @Override
    public JavaRDD<UnorderStateI> queryUnorderStateI(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/declare/unorder/i.parquet/");

        return sparkSession.read()
        .parquet(path)
        .as(Encoders.bean(UnorderStateI.class))
        .toJavaRDD();
    }


    @Override
    public JavaRDD<UnorderStateU> queryUnorderStateU(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/declare/unorder/u.parquet/");

        return sparkSession.read()
        .parquet(path)
        .as(Encoders.bean(UnorderStateU.class))
        .toJavaRDD();
    }


    @Override
    public JavaRDD<OrderState> queryOrderState(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/declare/order.parquet");

        return sparkSession.read()
        .parquet(path)
        .as(Encoders.bean(OrderState.class))
        .toJavaRDD();
    }


    @Override
    public JavaRDD<NegativeState> queryNegativeState(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/declare/negatives.parquet");

        return sparkSession.read()
        .parquet(path)
        .as(Encoders.bean(NegativeState.class))
        .toJavaRDD();
    }


}
