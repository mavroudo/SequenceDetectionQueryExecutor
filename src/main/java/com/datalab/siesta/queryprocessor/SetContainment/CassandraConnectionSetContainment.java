package com.datalab.siesta.queryprocessor.SetContainment;

import com.datalab.siesta.queryprocessor.Signatures.Signature;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Patterns.ComplexPattern;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.sql.Timestamp;
import java.util.*;

@Configuration
@PropertySource("classpath:application.properties")
@ConditionalOnProperty(
        value = "database",
        havingValue = "cassandra-rdd"
)
@Service
public class CassandraConnectionSetContainment {

    private SparkSession sparkSession;
    private JavaSparkContext javaSparkContext;

    @Autowired
    public CassandraConnectionSetContainment(SparkSession sparkSession, JavaSparkContext javaSparkContext) {
        this.sparkSession = sparkSession;
        this.javaSparkContext = javaSparkContext;
    }

    public List<Long> getPossibleTraceIds(ComplexPattern pattern, String logname) {
        String path = String.format("%s_set_idx", logname);
        Broadcast<Set<String>> events = javaSparkContext.broadcast(pattern.getEventTypes());
        List<Long> possibleTraces = this.sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", path, "keyspace", "siesta"))
                .load().toJavaRDD()
                .filter((Function<Row, Boolean>) row-> events.value().contains((String) row.getAs("event_name")))
                .flatMap((FlatMapFunction<Row, Tuple2<Long,Integer>>)row->{
                    List<Tuple2<Long,Integer>> recs = new ArrayList<>();
                    List<String> s = JavaConverters.seqAsJavaList(row.getAs("sequences"));
                    s.forEach(x-> recs.add(new Tuple2<>(Long.parseLong(x),1)));
                    return recs.iterator();
                } )
                .keyBy((Function<Tuple2<Long, Integer>, Long>) x -> x._1 )
                .reduceByKey((Function2<Tuple2<Long, Integer>, Tuple2<Long, Integer>, Tuple2<Long, Integer>>)
                        (a,b)-> new Tuple2<>(a._1,a._2+b._2))
                .filter((Function<Tuple2<Long, Tuple2<Long, Integer>>, Boolean>) rec -> rec._2._2==events.value().size())
                .map((Function<Tuple2<Long, Tuple2<Long, Integer>>, Long>) x->x._2._1 )
                .collect();
        return possibleTraces;
    }

    public Map<Long, List<Event>> getOriginalTraces(List<Long> traces, String logname) {
        String path = String.format("%s_set_seq", logname);
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
