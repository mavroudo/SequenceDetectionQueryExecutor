package com.datalab.siesta.queryprocessor.storage.repositories.CassandraRdd;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexPair;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.DBModel.Trace;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.repositories.SparkDatabaseRepository;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.rdd.ReadConf;
import com.datastax.spark.connector.types.TypeConverter;
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

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;


@Configuration
@ConditionalOnProperty(
        value = "database",
        havingValue = "cassandra-rdd"
)
@ComponentScan
public class CassConnector extends SparkDatabaseRepository{


    @Autowired
    public CassConnector(SparkSession sparkSession, JavaSparkContext javaSparkContext, Utils utils) {
        super(sparkSession, javaSparkContext, utils);
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
        Set<String> set = connector.withSessionDo(session -> session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = '"
                        + "siesta" + "';").all())
                .stream().map(x -> x.get("table_name", String.class)).filter(Objects::nonNull)
                .map(x ->
                        Arrays.stream(x.split("_")).
                                filter(y -> !keywords.contains(y)).collect(Collectors.joining("_"))
                ).collect(Collectors.toSet());
        return set;
    }

    @Override
    public List<Count> getCountForExploration(String logname, String event) {
        String path = String.format("%s_count", logname);
        Broadcast<String> bEventName = javaSparkContext.broadcast(event);
        List<Count> l = sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", path, "keyspace", "siesta"))
                .option("spark.cassandra.read.timeoutMS","120000")
                .load().toJavaRDD()
                .filter((Function<Row, Boolean>) row -> bEventName.value().equals(row.getString(0)))
                .flatMap((FlatMapFunction<Row, Count>) r -> {
                    String evA = r.getString(0);
                    List<Count> cs = new ArrayList<>();
                    List<String> rec = JavaConverters.seqAsJavaList(r.getSeq(1));
                    for (String record : rec) {
                        String[] c = record.split(",");
                        cs.add(new Count(evA, c));
                    }
                    return cs.iterator();
                }).collect();
        return new ArrayList<>(l);
    }

    @Override
    public List<Count> getCounts(String logname, Set<EventPair> pairs) {
        String path = String.format("%s_count", logname);
        Broadcast<Set<EventPair>> bEvents = javaSparkContext.broadcast(pairs);
        Broadcast<Set<String>> firstEvents = javaSparkContext.broadcast(pairs.stream()
                .map(s -> s.getEventA().getName()).collect(Collectors.toSet()));
        List<Count> l = sparkSession.read()
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
        return new ArrayList<>(l);
    }

//    @Override
//    public List<String> getEventNames(String logname) {
//        String path = String.format("%s_count", logname);
//        return sparkSession.read()
//                .format("org.apache.spark.sql.cassandra")
//                .options(Map.of("table", path, "keyspace", "siesta"))
//                .load()
//                .select("event_a")
//                .distinct()
//                .toJavaRDD()
//                .map((Function<Row, String>) r -> r.getString(0))
//                .collect();
//    }

    @Override
    public List<String> getEventNames(String logname){
        String path = String.format("%s_count", logname);
        JavaRDD<String> cassandraRowsRDD = javaFunctions(this.sparkSession.sparkContext())
                .cassandraTable("siesta", path)
                .select("event_a")
                .map((Function<CassandraRow, String>) row-> row.getString(0));
        List<String> s = cassandraRowsRDD.collect();
        return s;
    }

    @Override
    protected JavaRDD<EventBoth> getFromSingle(String logname, Set<Long> traceIds, Set<String> eventTypes) {
        String path = String.format("%s_single", logname);
        Broadcast<Set<Long>> bTraceIds = javaSparkContext.broadcast(traceIds);
        Broadcast<Set<String>> bEventTypes = javaSparkContext.broadcast(eventTypes);
        return sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", path, "keyspace", "siesta"))
                .load().toJavaRDD()
                .filter((Function<Row, Boolean>) row -> bEventTypes.value().contains(row.getString(0)))
                .flatMap((FlatMapFunction<Row, EventBoth>) row -> {
                    String eventType = row.getString(0);
                    List<EventBoth> events = new ArrayList<>();
                    List<String> occurrences = JavaConverters.seqAsJavaList(row.getSeq(2));
                    Long trace_id = row.getAs("trace_id");
                    if (bTraceIds.value().contains(trace_id)) {
                        for (String occ : occurrences) {
                            String[] o = occ.split(",");
                            events.add(new EventBoth(eventType, trace_id, Timestamp.valueOf(o[1]), Integer.parseInt(o[0])));
                        }
                    }
                    return events.iterator();
                });
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
    protected JavaPairRDD<Tuple2<String, String>, Iterable<IndexPair>> getAllEventPairs(Set<EventPair> pairs,
                                                                                        String logname, Metadata metadata,
                                                                                        Timestamp from, Timestamp till) {
        String path = String.format("%s_index", logname);
        Broadcast<Set<EventPair>> bPairs = javaSparkContext.broadcast(pairs);
        Broadcast<String> mode = javaSparkContext.broadcast(metadata.getMode());
        Broadcast<Timestamp> bFrom = javaSparkContext.broadcast(from);
        Broadcast<Timestamp> bTill = javaSparkContext.broadcast(till);
        return sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", path, "keyspace", "siesta"))
                .option("spark.cassandra.connection.connections_per_executor_max_local","2")
                .load().toJavaRDD()
                .filter((Function<Row, Boolean>) row -> {
                    Timestamp start = row.getAs("start");
                    Timestamp end = row.getAs("end");
                    if (bFrom.value() != null && bFrom.value().after(end)) return false;
                    if (bTill.value() != null && bTill.value().before(start)) return false;
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
                            if(mode.value().equals("timestamps")) {
                                indexPairs.add(new IndexPair(trace_id, eventA, eventB, Timestamp.valueOf(f[0]),
                                        Timestamp.valueOf(f[1])));
                            }else{
                                indexPairs.add(new IndexPair(trace_id, eventA, eventB, Integer.parseInt(f[0]),
                                        Integer.parseInt(f[1])));
                            }

                        }
                    }
                    return indexPairs.iterator();
                })
                .filter((Function<IndexPair, Boolean>) indexPairs -> indexPairs.validate(bPairs.getValue()))
                .filter((Function<IndexPair, Boolean>) p->{
                    if(mode.value().equals("timestamps")) {
                        if(bTill.value()!=null && p.getTimestampA().after(bTill.value())) return false;
                        if(bFrom.value()!=null && p.getTimestampB().before(bFrom.value())) return false;
                    }
                    //If from and till has been set we cannot check it here
                    return true;
                })
                .groupBy((Function<IndexPair, Tuple2<String, String>>) indexPair -> new Tuple2<>(indexPair.getEventA(), indexPair.getEventB()));
    }

//    @Override
//    protected JavaPairRDD<Tuple2<String, String>, Iterable<IndexPair>> getAllEventPairs(Set<EventPair> pairs,
//                                                                                        String logname, Metadata metadata,
//                                                                                        Timestamp from, Timestamp till) {
//        String path = String.format("%s_index", logname);
//        Broadcast<Set<EventPair>> bPairs = javaSparkContext.broadcast(pairs);
//        Broadcast<String> mode = javaSparkContext.broadcast(metadata.getMode());
//        Broadcast<Timestamp> bFrom = javaSparkContext.broadcast(from);
//        Broadcast<Timestamp> bTill = javaSparkContext.broadcast(till);
//
//        CassandraConnector connector = CassandraConnector.apply(sparkSession.sparkContext().getConf());
//
//        JavaRDD<IndexRow> ir = javaFunctions(this.sparkSession.sparkContext())
//                .cassandraTable("siesta",path,mapRowTo(IndexRow.class))
//                .withConnector(connector)
//                .filter((Function<IndexRow, Boolean>) row->{
//                    if (bFrom.value() != null && bFrom.value().after(row.getEnd())) return false;
//                    if (bTill.value() != null && bTill.value().before(row.getStart())) return false;
//                    return true;
//                });
//
//        System.out.println(ir.take(5));
//
//        return sparkSession.read()
//                .format("org.apache.spark.sql.cassandra")
//                .options(Map.of("table", path, "keyspace", "siesta"))
//                .option("spark.cassandra.connection.connections_per_executor_max_local","2")
//                .load().toJavaRDD()
//                .filter((Function<Row, Boolean>) row -> {
//                    Timestamp start = row.getAs("start");
//                    Timestamp end = row.getAs("end");
//                    if (bFrom.value() != null && bFrom.value().after(end)) return false;
//                    if (bTill.value() != null && bTill.value().before(start)) return false;
//                    return true;
//                })
//                .flatMap((FlatMapFunction<Row, IndexPair>) row -> {
//                    String eventA = row.getAs("event_a");
//                    String eventB = row.getAs("event_b");
//                    List<String> ocs = JavaConverters.seqAsJavaList(row.getSeq(4));
//                    List<IndexPair> indexPairs = new ArrayList<>();
//                    for (String trace : ocs) {
//                        String[] split = trace.split("\\|\\|");
//                        long trace_id = Long.parseLong(split[0]);
//                        String[] p_split = split[1].split(",");
//                        for (String p : p_split) {
//                            String[] f = p.split("\\|");
//                            if(mode.value().equals("timestamps")) {
//                                indexPairs.add(new IndexPair(trace_id, eventA, eventB, Timestamp.valueOf(f[0]),
//                                        Timestamp.valueOf(f[1])));
//                            }else{
//                                indexPairs.add(new IndexPair(trace_id, eventA, eventB, Integer.parseInt(f[0]),
//                                        Integer.parseInt(f[1])));
//                            }
//
//                        }
//                    }
//                    return indexPairs.iterator();
//                })
//                .filter((Function<IndexPair, Boolean>) indexPairs -> indexPairs.validate(bPairs.getValue()))
//                .filter((Function<IndexPair, Boolean>) p->{
//                    if(mode.value().equals("timestamps")) {
//                        if(bTill.value()!=null && p.getTimestampA().after(bTill.value())) return false;
//                        if(bFrom.value()!=null && p.getTimestampB().before(bFrom.value())) return false;
//                    }
//                    //If from and till has been set we cannot check it here
//                    return true;
//                })
//                .groupBy((Function<IndexPair, Tuple2<String, String>>) indexPair -> new Tuple2<>(indexPair.getEventA(), indexPair.getEventB()));
//    }


}
