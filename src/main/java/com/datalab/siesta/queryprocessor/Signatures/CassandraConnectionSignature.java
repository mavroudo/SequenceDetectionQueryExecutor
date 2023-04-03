package com.datalab.siesta.queryprocessor.Signatures;

import com.datalab.siesta.queryprocessor.model.DBModel.EventTypes;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.ExtractedPairsForPatternDetection;
import com.datalab.siesta.queryprocessor.model.Patterns.ComplexPattern;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;


@Configuration
@PropertySource("classpath:application.properties")
@ConditionalOnProperty(
        value = "database",
        havingValue = "cassandra-rdd"
)
@Service
public class CassandraConnectionSignature {

    private SparkSession sparkSession;
    private JavaSparkContext javaSparkContext;

    @Autowired
    public CassandraConnectionSignature(SparkSession sparkSession, JavaSparkContext javaSparkContext) {
        this.sparkSession = sparkSession;
        this.javaSparkContext = javaSparkContext;
    }

    public Signature getSignature(String logname) {
        String path = String.format("%s_sign_meta", logname);
        List<Row> rows = this.sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", path, "keyspace", "siesta"))
                .load().toJavaRDD().collect();
        Signature signature = new Signature();
        for (Row r : rows) {
            if (r.getString(0).equals("events")) {
                List<String> events = JavaConverters.seqAsJavaList(r.getSeq(1));
                signature.setEvents(events);
            } else {
                List<String> pairs = JavaConverters.seqAsJavaList(r.getSeq(1));
                List<EventTypes> eventPairs = pairs.stream().map(x -> {
                    String[] types = x.split(",");
                    return new EventTypes(types[0], types[1]);
                }).collect(Collectors.toList());
                signature.setEventPairs(eventPairs);
            }
        }
        return signature;

    }

    public List<Long> getPossibleTraceIds(ComplexPattern pattern, String logname, Signature s) {
        String path = String.format("%s_sign_idx", logname);
        ExtractedPairsForPatternDetection pairs = pattern.extractPairsForPatternDetection(false);
        Set<Integer> positions1 = s.findPositionsWith1(pattern.getEventTypes(), pairs.getAllPairs());
        Iterator<Integer> iter = positions1.iterator();
        List<String> conditions = new ArrayList<>();
        while (iter.hasNext()) {
            conditions.add(" signature[" + iter.next().toString() + "]=" + "'1' ");
        }
        return sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", path, "keyspace", "siesta"))
                .load()
                .where(String.join("and", conditions))
                .toJavaRDD()
                .flatMap((FlatMapFunction<Row, Long>) row -> {
                    List<String> traces = JavaConverters.seqAsJavaList(row.getSeq(1));
                    return traces.stream().map(Long::parseLong).collect(Collectors.toList()).iterator();
                }).collect();
    }

    public Map<Long, List<Event>> getOriginalTraces(List<Long> traces, String logname) {
        String path = String.format("%s_sign_seq", logname);
        Broadcast<Set<Long>> bTraces = javaSparkContext.broadcast(new HashSet<>(traces));
        return sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", path, "keyspace", "siesta"))
                .load().toJavaRDD()
                .filter((Function<Row, Boolean>) row -> bTraces.value()
                        .contains(Long.parseLong(row.getAs("sequence_id"))))
                .map((Function<Row, Tuple2<Long,List<Event>>>) row->{
                    Long trace_id = Long.parseLong(row.getAs("sequence_id"));
                    List<String> events = JavaConverters.seqAsJavaList(row.getSeq(1));
                    List<Event> response = new ArrayList<>();
                    for(int i=0;i<events.size();i++){
                        String[] e = events.get(i).split("\\(")[1].replace(")","").split(",");
                        response.add(new EventBoth(e[1],trace_id, Timestamp.valueOf(e[0]),i));
                    }
                    return new Tuple2<>(trace_id,response);
                } ).keyBy((Function<Tuple2<Long, List<Event>>, Long>) x->x._1 )
                .mapValues((Function<Tuple2<Long, List<Event>>, List<Event>>) x->x._2 )
                .collectAsMap();
    }


}
