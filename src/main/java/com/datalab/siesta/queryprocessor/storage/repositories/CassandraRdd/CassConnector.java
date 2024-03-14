package com.datalab.siesta.queryprocessor.storage.repositories.CassandraRdd;

import com.datalab.siesta.queryprocessor.declare.model.EventPairToTrace;
import com.datalab.siesta.queryprocessor.declare.model.OccurrencesPerTrace;
import com.datalab.siesta.queryprocessor.declare.model.UniqueTracesPerEventPair;
import com.datalab.siesta.queryprocessor.declare.model.UniqueTracesPerEventType;
import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexPair;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.DBModel.Trace;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.repositories.SparkDatabaseRepository;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraRow;
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
import scala.Tuple3;
import scala.collection.JavaConverters;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;


/**
 * Main class that describes the connection of Query Processor to Cassandra
 */
@Configuration
@ConditionalOnProperty(
        value = "database",
        havingValue = "cassandra-rdd"
)
@ComponentScan
public class CassConnector extends SparkDatabaseRepository {


    @Autowired
    public CassConnector(SparkSession sparkSession, JavaSparkContext javaSparkContext, Utils utils) {
        super(sparkSession, javaSparkContext, utils);
    }

    /**
     *
     * @param logname the log database
     * @return a list with all the event types stored in it
     */
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

    /**
     * @return a set with all the stored log databases
     */
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


    /**
     * For a given event type inside a log database, returns all the possible next events. That is, since Count
     * contains for each pair the stats, return all the events that have at least one pair with the given event
     * @param logname the log database
     * @param event the event type
     * @return the possible next events
     */
    @Override
    public List<Count> getCountForExploration(String logname, String event) {
        String path = String.format("%s_count", logname);
        Broadcast<String> bEventName = javaSparkContext.broadcast(event);
        List<Count> l = sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", path, "keyspace", "siesta"))
                .option("spark.cassandra.read.timeoutMS", "120000")
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

    /**
     * Retrieves the corresponding stats (min, max duration and so on) from the CountTable, for a given set of event
     * pairs
     * @param logname the log database
     * @param pairs a set with the event pairs
     * @return a list of the stats for the set of event pairs
     */
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


    /**
     *
     * @param logname the log database
     * @return a list with all the event types stored in it
     */
    @Override
    public List<String> getEventNames(String logname) {
        String path = String.format("%s_count", logname);
        JavaRDD<String> cassandraRowsRDD = javaFunctions(this.sparkSession.sparkContext())
                .cassandraTable("siesta", path)
                .select("event_a")
                .map((Function<CassandraRow, String>) row -> row.getString(0));
        List<String> s = cassandraRowsRDD.collect();
        return s;
    }

    /**
     * Should be overridden by any storage that uses spark
     *
     * @param logname    The name of the Log
     * @param traceIds   The traces we want to detect
     * @param eventTypes The event types to be collected
     * @return a JavaRDD<EventBoth> that will be used in querySingleTable and querySingleTableGroups
     */
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

    /**
     * This function reads data from the Sequence table into a JavaRDD, any database that utilizes spark should
     * override it
     *
     * @param logname   Name of the log
     * @param bTraceIds broadcasted the values of the trace ids we are interested in
     * @return a JavaRDD<Trace>
     */
    @Override
    protected JavaRDD<Trace> querySequenceTablePrivate(String logname, Broadcast<Set<Long>> bTraceIds) {
        return this.querySequenceTableDeclare(logname)
                .filter((Function<Trace, Boolean>) trace -> bTraceIds.getValue().contains(trace.getTraceID()));
    }

    /**
     * return all the IndexPairs grouped by the eventA and eventB
     *
     * @param pairs set of the pairs
     * @param logname the log database
     * @return extract the pairs
     */
    @Override
    protected JavaPairRDD<Tuple2<String, String>, Iterable<IndexPair>> getAllEventPairs(Set<EventPair> pairs,
                                                                                        String logname, Metadata metadata,
                                                                                        Timestamp from, Timestamp till) {
        String path = String.format("%s_index", logname);
        Broadcast<Set<EventPair>> bPairs = javaSparkContext.broadcast(pairs);
        Broadcast<String> mode = javaSparkContext.broadcast(metadata.getMode());
        Broadcast<Timestamp> bFrom = javaSparkContext.broadcast(from);
        Broadcast<Timestamp> bTill = javaSparkContext.broadcast(till);


        String eventPairsQuery = pairs.stream()
                .map(pair -> String.format("token(\"event_a\",\"event_b\")=token('%s','%s')", pair.getEventA().getName(), pair.getEventB().getName()))
                .collect(Collectors.joining(" or "));


        CassandraConnector connector = CassandraConnector.apply(sparkSession.sparkContext().getConf());
        List<String> queries = pairs.stream().map(p -> {
            return String.format("select * from siesta.%s where token(\"event_a\",\"event_b\")=token('%s','%s');",
                    path, p.getEventA().getName(), p.getEventB().getName());
        }).collect(Collectors.toList());
        CodecRegistry codecRegistry = CodecRegistry.DEFAULT;

        List<IndexRow> indexRows = new ArrayList<>();
        for (String query : queries) {
            connector.withSessionDo(session -> session.execute(query)).all().stream().map(row -> {
                Timestamp tStart = Timestamp.from(row.getInstant("start"));
                Timestamp tEnd = Timestamp.from(row.getInstant("end"));
                return new IndexRow(row.getString("event_a"), row.getString("event_b"),
                        tStart, tEnd,
                        row.getList("occurrences", String.class));
            }).forEach(indexRows::add);
        }

        JavaRDD<IndexRow> rddIndexRow = javaSparkContext.parallelize(indexRows);

        return rddIndexRow.filter((Function<IndexRow, Boolean>) row -> {
                    if (bFrom.value() != null && bFrom.value().after(row.getEnd())) return false;
                    if (bTill.value() != null && bTill.value().before(row.getStart())) return false;
                    return true;
                })
                .flatMap((FlatMapFunction<IndexRow, IndexPair>) row -> row.extractOccurrences(mode, bFrom, bTill).iterator())
                .groupBy((Function<IndexPair, Tuple2<String, String>>) indexPair -> new Tuple2<>(indexPair.getEventA(), indexPair.getEventB()));

    }

    @Override
    public JavaRDD<Trace> querySequenceTableDeclare(String logname) {
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
                });
    }

    @Override
    public JavaRDD<UniqueTracesPerEventType> querySingleTableDeclare(String logname) {
        String path = String.format("%s_single", logname);
        JavaRDD<Row> rows = sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", path, "keyspace", "siesta"))
                .load().toJavaRDD();

        return rows.map(row -> {
                    String eventType = row.getString(0);
                    Long trace_id = row.getAs("trace_id");
                    List<String> occurrences = JavaConverters.seqAsJavaList(row.getSeq(2));
                    return new Tuple3<>(eventType, trace_id, occurrences.size());
                })
                .groupBy(Tuple3::_1)
                .map(x -> {
                    List<OccurrencesPerTrace> occs = new ArrayList<>();
                    x._2.forEach(y -> occs.add(new OccurrencesPerTrace(y._2(), y._3())));
                    return new UniqueTracesPerEventType(x._1, occs);
                });
    }


    @Override
    public JavaRDD<UniqueTracesPerEventPair> queryIndexTableDeclare(String logname) {
        String path = String.format("%s_index", logname);
        JavaRDD<Row> rows = sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", path, "keyspace", "siesta"))
                .load().toJavaRDD();
        return rows.map(row -> {
            String evA = row.getString(0);
            String evB = row.getString(1);
            List<String> occurrences = JavaConverters.seqAsJavaList(row.getSeq(4));
            List<Long> uniqueTraces = new ArrayList<>();
            occurrences.forEach(x -> {
                Long t = Long.valueOf(x.split("\\|\\|")[0]);
                uniqueTraces.add(t);
            });
            return new UniqueTracesPerEventPair(evA, evB, uniqueTraces);
        });
    }

    @Override
    public JavaRDD<IndexPair> queryIndexTableAllDeclare(String logname) {
        String path = String.format("%s_index", logname);
        JavaRDD<Row> rows = sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", path, "keyspace", "siesta"))
                .load().toJavaRDD();
        JavaRDD<IndexPair> indexPairsRDD = rows.flatMap(row -> {
            String evA = row.getString(0);
            String evB = row.getString(1);
            List<String> occurrences = JavaConverters.seqAsJavaList(row.getSeq(4));
            List<IndexPair> indexPairs = new ArrayList<>();
            //assume that we operate only in positions (check if this needs to be modified in the future)
            occurrences.forEach(x -> {
                Long t = Long.valueOf(x.split("\\|\\|")[0]);
                String[] ocs = x.split("\\|\\|")[1].split(",");
                for (int i = 0; i < ocs.length; i++) {
                    String[] spl = ocs[i].split("\\|");
                    indexPairs.add(new IndexPair(t, evA, evB, Integer.parseInt(spl[0]), Integer.parseInt(spl[1])));
                }
            });
            return indexPairs.iterator();
        });
        return indexPairsRDD;
    }

    /**
     * <String,Long>,List<Integer> = <Event type, Trace_id>, positions of event occurrences
     * @param logname
     * @return
     */
    @Override
    public JavaPairRDD<Tuple2<String, Long>, List<Integer>> querySingleTableAllDeclare(String logname) {

        String path = String.format("%s_single", logname);
        JavaRDD<Row> rows = sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", path, "keyspace", "siesta"))
                .load().toJavaRDD();

        return rows.map(row -> {
                    String eventType = row.getString(0);
                    Long trace_id = row.getAs("trace_id");
                    List<String> occurrences = JavaConverters.seqAsJavaList(row.getSeq(2));
                    List<Integer> positions = new ArrayList<>();
                    for(String oc:occurrences){
                        positions.add(Integer.valueOf(oc.split(",")[0]));
                    }
                    return new Tuple3<>(eventType, trace_id, positions);
                })
                .keyBy(x->new Tuple2<String,Long>(x._1(),x._2()))
                .mapValues(x->x._3());

    }

    /**
     * <Event type A, Event Type B, Trace_id>
     *
     * @param logname
     * @return
     */
    @Override
    public JavaRDD<EventPairToTrace> queryIndexOriginalDeclare(String logname) {
        String path = String.format("%s_index", logname);
        JavaRDD<Row> rows = sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", path, "keyspace", "siesta"))
                .load().toJavaRDD();
        JavaRDD<EventPairToTrace> indexPairsRDD = rows.flatMap(row -> {
            String evA = row.getString(0);
            String evB = row.getString(1);
            List<String> occurrences = JavaConverters.seqAsJavaList(row.getSeq(4));
            List<EventPairToTrace> eventPairToTraces = new ArrayList<>();
            //assume that we operate only in positions (check if this needs to be modified in the future)
            occurrences.forEach(x -> {
                Long t = Long.valueOf(x.split("\\|\\|")[0]);
                eventPairToTraces.add(new EventPairToTrace(evA,evB,t));
            });
            return eventPairToTraces.iterator();
        });
        return indexPairsRDD;

    }

}
