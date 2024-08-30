package com.datalab.siesta.queryprocessor.storage.repositories.S3;

import com.datalab.siesta.queryprocessor.declare.model.EventPairToTrace;
import com.datalab.siesta.queryprocessor.declare.model.OccurrencesPerTrace;
import com.datalab.siesta.queryprocessor.declare.model.UniqueTracesPerEventPair;
import com.datalab.siesta.queryprocessor.declare.model.UniqueTracesPerEventType;
import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexPair;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.DBModel.Trace;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.repositories.SparkDatabaseRepository;
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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.JavaConverters;
import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
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
public class S3Connector extends SparkDatabaseRepository{


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
        S3Functions s3Functions = new S3Functions();
        return S3Functions.findAllLongNames(this.bucket,sparkSession);
//
//        try {
//            FileSystem fs = FileSystem.get(new URI(this.bucket), sparkSession.sparkContext().hadoopConfiguration());
//            RemoteIterator<LocatedFileStatus> f = fs.listFiles(new Path(this.bucket), true);
//            Pattern pattern = Pattern.compile(String.format("%s[^/]*/", this.bucket));
//            Set<String> files = new HashSet<>();
//            while (f.hasNext()) {
//                LocatedFileStatus fin = f.next();
//                Matcher matcher = pattern.matcher(fin.getPath().toString());
//                if (matcher.find()) {
//                    String logname = matcher.group(0).replace(this.bucket, "").replace("/", "");
//                    files.add(logname);
//                }
//            }
//            return files;
//
//        } catch (IOException | URISyntaxException e) {
//            throw new RuntimeException(e);
//        }
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
                        long max_daration = v1.getLong(4);
                        c.add(new Count(eventA, eventB, sum_duration, count, min_duration, max_daration));
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
                        long max_daration = v1.getLong(4);
                        c.add(new Count(eventA, eventB, sum_duration, count, min_duration, max_daration));
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
    public List<String> getEventNames(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/count.parquet/");
        return sparkSession.read().parquet(path)
                .select("eventA")
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
                .filter((Function<Row, Boolean>) x -> bEventTypes.value().contains(x.getString(1)))
                .flatMap((FlatMapFunction<Row, EventBoth>) row -> {
                    String eventType = row.getString(1);
                    List<EventBoth> events = new ArrayList<>();
                    List<Row> occurrences = JavaConverters.seqAsJavaList(row.getSeq(0));
                    for (Row occurrence : occurrences) {
                        String traceId = occurrence.getString(0);
                        List<String> times = JavaConverters.seqAsJavaList(occurrence.getSeq(1));
                        List<Integer> positions = JavaConverters.seqAsJavaList(occurrence.getSeq(2));
                        for (int i = 0; i < times.size(); i++) {
                            events.add(new EventBoth(eventType, traceId, Timestamp.valueOf(times.get(i)), positions.get(i)));
                        }
                    }
                    return events.iterator();
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
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (from != null) {
            whereStatements.add(String.format("start >= '%s' ", dateFormat.format(new Date(from.getTime()))));
        }
        if (till != null) {
            whereStatements.add(String.format("end <= '%s' ", dateFormat.format(new Date(till.getTime()))));
        }
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
                        }
                    }
                    List<IndexPair> response = new ArrayList<>();
                    if (checkContained) {
                        List<Row> l = JavaConverters.seqAsJavaList(row.getSeq(1));
                        for (Row r2 : l) {
                            String tid = r2.getString(0);
                            List<Row> innerList = JavaConverters.seqAsJavaList(r2.getSeq(1));
                            if (mode.getValue().equals("positions")) {
                                for (Row inner : innerList) {
                                    int posA = inner.getInt(0);
                                    int posB = inner.getInt(1);
                                    response.add(new IndexPair(tid, eventA, eventB, posA, posB));
                                }
                            } else {
                                for (Row inner : innerList) {
                                    Timestamp tsA = Timestamp.valueOf(inner.getString(0));
                                    Timestamp tsB = Timestamp.valueOf(inner.getString(1));
                                    if (!(bTill.value() != null && tsA.after(bTill.value()) ||
                                            bFrom.value() != null && tsB.before(bFrom.value()))) {
                                        response.add(new IndexPair(tid, eventA, eventB, tsA, tsB));
                                    }
                                }
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
                .map((Function<Row, Trace>) row -> {
                    String trace_id = row.getAs("trace_id");
                    List<Row> evs = JavaConverters.seqAsJavaList(row.getSeq(1));
                    List<EventBoth> results = new ArrayList<>();
                    for (int i = 0; i < evs.size(); i++) {
                        String event_name = evs.get(i).getString(0);
                        Timestamp event_timestamp = Timestamp.valueOf(evs.get(i).getString(1));
                        results.add(new EventBoth(event_name, event_timestamp, i));
                    }
                    return new Trace(trace_id, results);
                });
    }

    @Override
    public JavaRDD<UniqueTracesPerEventType> querySingleTableDeclare(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/single.parquet/");

        return sparkSession.read()
                .parquet(path)
                .toJavaRDD()
                .map(row -> {
                    UniqueTracesPerEventType ue = new UniqueTracesPerEventType();
                    ue.setEventType(row.getString(1));
                    List<OccurrencesPerTrace> ocs = new ArrayList<>();
                    List<Row> occurrences = JavaConverters.seqAsJavaList(row.getSeq(0));
                    for (Row occurrence : occurrences) {
                        String traceId = occurrence.getString(0);
                        int size = JavaConverters.seqAsJavaList(occurrence.getSeq(2)).size();
                        ocs.add(new OccurrencesPerTrace(traceId, size));
                    }
                    ue.setOccurrences(ocs);
                    return ue;
                });
    }

    @Override
    public JavaPairRDD<Tuple2<String, String>, List<Integer>> querySingleTableAllDeclare(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/single.parquet/");

        return sparkSession.read()
                .parquet(path)
                .toJavaRDD()
                .flatMap((FlatMapFunction<Row, Tuple3<String, String, List<Integer>>>) row -> {
                    String eventType = row.getString(1);
                    List<Tuple3<String, String, List<Integer>>> records = new ArrayList<>();
                    List<Row> occurrences = JavaConverters.seqAsJavaList(row.getSeq(0));
                    for (Row occurrence : occurrences) {
                        String tid = occurrence.getString(0);
                        List<Integer> positions = JavaConverters.seqAsJavaList(occurrence.getSeq(2));
                        records.add(new Tuple3<>(eventType, tid, positions));
                    }
                    return records.iterator();
                })
                .keyBy(r -> new Tuple2<>(r._1(), r._2()))
                .mapValues(Tuple3::_3);
    }

    @Override
    public JavaRDD<EventPairToTrace> queryIndexOriginalDeclare(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/index.parquet/");

        return sparkSession.read()
                .parquet(path)
                .toJavaRDD()
                .flatMap((FlatMapFunction<Row, EventPairToTrace>) row -> {
                    String eventA = row.getAs("eventA");
                    String eventB = row.getAs("eventB");
                    List<EventPairToTrace> response = new ArrayList<>();
                    List<Row> l = JavaConverters.seqAsJavaList(row.getSeq(1));
                    for (Row r2 : l) {
                        String tid = r2.getString(0);
                        response.add(new EventPairToTrace(eventA,eventB,tid));
                    }
                    return response.iterator();
                })
                .distinct();
    }

    @Override
    public JavaRDD<UniqueTracesPerEventPair> queryIndexTableDeclare(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/index.parquet/");

        return sparkSession.read()
                .parquet(path)
                .toJavaRDD()
                .map(row -> {
                    String eventA = row.getAs("eventA");
                    String eventB = row.getAs("eventB");
                    List<String> uniqueTraces = new ArrayList<>();

                    List<Row> l = JavaConverters.seqAsJavaList(row.getSeq(1));
                    for (Row r2 : l) {
                        String tid = r2.getString(0);
                        uniqueTraces.add(tid);
                    }
                    return new UniqueTracesPerEventPair(eventA, eventB, uniqueTraces);
                })
                .keyBy(x->new Tuple2<>(x.getEventA(),x.getEventB()))
                .reduceByKey((x,y)->{
                    x.getUniqueTraces().addAll(y.getUniqueTraces());
                    return x;
                        }
                ).map(x->x._2);
    }

    @Override
    public JavaRDD<IndexPair> queryIndexTableAllDeclare(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/index.parquet/");

        return sparkSession.read()
                .parquet(path)
                .toJavaRDD()
                .flatMap((FlatMapFunction<Row, IndexPair>) row -> {
                    String eventA = row.getAs("eventA");
                    String eventB = row.getAs("eventB");
                    List<IndexPair> response = new ArrayList<>();

                    List<Row> l = JavaConverters.seqAsJavaList(row.getSeq(1));
                    for (Row r2 : l) {
                        String tid = r2.getString(0);
                        List<Row> innerList = JavaConverters.seqAsJavaList(r2.getSeq(1));
                        for (Row inner : innerList) {
                            int posA = inner.getInt(0);
                            int posB = inner.getInt(1);
                            response.add(new IndexPair(tid, eventA, eventB, posA, posB));
                        }
                    }
                    return response.iterator();
                });
    }
}
