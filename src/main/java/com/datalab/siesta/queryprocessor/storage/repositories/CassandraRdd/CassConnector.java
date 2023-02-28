package com.datalab.siesta.queryprocessor.storage.repositories.CassandraRdd;

import com.datalab.siesta.queryprocessor.model.DBModel.*;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.DatabaseRepository;
import com.datalab.siesta.queryprocessor.storage.repositories.SparkDatabaseRepository;
import com.datastax.spark.connector.cql.CassandraConnector;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
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
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;


@Configuration
@ConditionalOnProperty(
        value = "database",
        havingValue = "cassandra-rdd",
        matchIfMissing = true
)
@ComponentScan
public class CassConnector extends SparkDatabaseRepository {


    @Autowired
    public CassConnector(SparkSession sparkSession, JavaSparkContext javaSparkContext) {
        super(sparkSession, javaSparkContext);
    }

    @Override
    public Metadata getMetadata(String logname) {
        Dataset<Row> df = sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", logname + "_meta", "keyspace", "siesta"))
                .load();
        Map<String, String> m = new HashMap<>();
        df.toJavaRDD().map((Function<Row, Tuple2<String, String>>) row ->
                        new Tuple2<>(row.getString(0), row.getString(1)))
                .collect().forEach(t -> {
                    m.put(t._1, t._2);
                });
        return new Metadata(m);
    }

    @Override
    public Set<String> findAllLongNames() {
        CassandraConnector connector = CassandraConnector.apply(sparkSession.sparkContext().getConf());
        List<String> keywords = new ArrayList<>() {{
            add("set");
            add("sign");
            add("meta");
            add("idx");
            add("count");
            add("index");
            add("seq");
            add("lastchecked");
            add("single");
        }};
        return connector.withSessionDo(session -> session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = '"
                        + "siesta" + "';").all())
                .stream().map(x -> x.get("table_name", String.class)).filter(Objects::nonNull)
                .map(x ->
                        Arrays.stream(x.split("_")).
                                filter(y -> !keywords.contains(y)).collect(Collectors.joining("_"))
                ).collect(Collectors.toSet());
    }

    @Override
    public List<Count> getCounts(String logname, Set<EventPair> pairs) {
        String path = String.format("%s_count", logname);
        Broadcast<Set<EventPair>> bEvents = javaSparkContext.broadcast(pairs);
        Broadcast<Set<String>> firstEvents = javaSparkContext.broadcast(pairs.stream()
                .map(s -> s.getEventA().getName()).collect(Collectors.toSet()));
        return sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", path, "keyspace", "siesta"))
                .load().toJavaRDD()
                .filter((Function<Row, Boolean>) row -> firstEvents.getValue().contains(row.getString(0)))
                .flatMap((FlatMapFunction<Row, Count>) r -> {
                    String evA = r.getString(0);
                    List<Count> cs = new ArrayList<>();
                    List<String> rec = JavaConverters.seqAsJavaList(r.getSeq(1));
                    for (String record : rec) {
                        String[] c = record.split(",");
                        cs.add(new Count(evA, c));
                    }
                    return cs.iterator();
                })
                .filter((Function<Count, Boolean>) c -> {
                    for (EventPair p : bEvents.getValue()) {
                        if (c.getEventA().equals(p.getEventA().getName()) && c.getEventB().equals(p.getEventB().getName())) {
                            return true;
                        }
                    }
                    return false;
                })
                .collect();
    }

    @Override
    public List<String> getEventNames(String logname) {
        String path = String.format("%s_single", logname);
        return sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", path, "keyspace", "siesta"))
                .load().toJavaRDD()
                .map((Function<Row, String>) r -> r.getString(0))
                .distinct()
                .collect();
    }


    @Override
    protected JavaRDD<Trace> querySequenceTablePrivate(String logname, Broadcast<Set<Long>> bTraceIds) {
        String path = String.format("%s_seq", logname);
        return sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", path, "keyspace", "siesta"))
                .load().toJavaRDD()
                .map((Function<Row, Trace>) row -> {
                    long trace_id = Long.parseLong(row.getAs("sequence_id"));
                    List<String> evs = JavaConverters.seqAsJavaList(row.getSeq(1));
                    List<EventBoth> results = new ArrayList<>();
                    int i = 0;
                    for (String ev : evs) {
                        String[] s = ev.split(",");
                        String event_name = s[1];
                        Timestamp event_timestamp = Timestamp.valueOf(s[0]);
                        results.add(new EventBoth(event_name, event_timestamp, i));
                        i++;
                    }
                    return new Trace(trace_id, results);
                })
                .filter((Function<Trace, Boolean>) trace -> bTraceIds.getValue().contains(trace.getTraceID()));
    }

    @Override
    protected JavaPairRDD<Tuple2<String, String>, Iterable<IndexPair>> getAllEventPairs(Set<EventPair> pairs, String logname, Metadata metadata) {
        String path = String.format("%s_index", logname);
        Broadcast<Set<EventPair>> bPairs = javaSparkContext.broadcast(pairs);
        Broadcast<String> mode = javaSparkContext.broadcast(metadata.getMode());
        return sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", path, "keyspace", "siesta"))
                .load().toJavaRDD()
                .filter((Function<Row, Boolean>) row -> { //TODO: add filtering here for the time-window (same position to S3)
                    Timestamp start = row.getAs("start");
                    Timestamp end = row.getAs("end");
                    return true;
                })
                .flatMap((FlatMapFunction<Row, IndexPair>) row -> {
                    String eventA = row.getAs("event_a");
                    String eventB = row.getAs("event_b");
                    List<String> ocs = JavaConverters.seqAsJavaList(row.getSeq(4));
                    List<IndexPair> indexPairs = new ArrayList<>();
                    for (String trace : ocs) {
                        String[] split = trace.split("\\|\\|");
                        long trace_id = Long.parseLong(split[0]);
                        String[] p_split = split[1].split(",");
                        for (String p : p_split) {
                            String[] f = p.split("\\|");
                            indexPairs.add(new IndexPair(trace_id, eventA, eventB, Timestamp.valueOf(f[0]),
                                    Timestamp.valueOf(f[1])));
                        }
                    }
                    return indexPairs.iterator();
                })
                .filter((Function<IndexPair, Boolean>) indexPairs -> indexPairs.validate(bPairs.getValue()))
                .groupBy((Function<IndexPair, Tuple2<String, String>>) indexPair -> new Tuple2<>(indexPair.getEventA(), indexPair.getEventB()));
    }


}
