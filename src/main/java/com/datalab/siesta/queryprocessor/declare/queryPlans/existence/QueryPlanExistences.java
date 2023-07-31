package com.datalab.siesta.queryprocessor.declare.queryPlans.existence;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.model.*;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseExistence;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.util.*;
import java.util.stream.Collectors;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueryPlanExistences {

    private DeclareDBConnector declareDBConnector;
    private JavaSparkContext javaSparkContext;
    //initialize a protected variable of the required events
    private QueryResponseExistence queryResponseExistence;
    private Metadata metadata;


    @Autowired
    public QueryPlanExistences(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext) {
        this.declareDBConnector = declareDBConnector;
        this.javaSparkContext = javaSparkContext;
        this.queryResponseExistence = new QueryResponseExistence();
    }

    public QueryResponse execute(String logname, List<String> modes, double support) {
        Broadcast<Double> bSupport = javaSparkContext.broadcast(support);
        Broadcast<Long> bTotalTraces = javaSparkContext.broadcast(metadata.getTraces());

        //if existence, absence or exactly in modes
        JavaRDD<UniqueTracesPerEventType> uEventType = declareDBConnector.querySingleTableDeclare(logname);
        uEventType.persist(StorageLevel.MEMORY_AND_DISK());
        Map<String, HashMap<Integer, Long>> groupTimes = this.createMapForSingle(uEventType);
        Map<String, Long> singleUnique = this.extractUniqueTracesSingle(groupTimes);
        Broadcast<Map<String, Long>> bUniqueSingle = javaSparkContext.broadcast(singleUnique);

        JavaRDD<UniqueTracesPerEventPair> uPairs = declareDBConnector.queryIndexTableDeclare(logname);
        uPairs.persist(StorageLevel.MEMORY_AND_DISK());
        JavaRDD<Tuple3<String, String, Integer>> joined = joinUnionTraces(uPairs);
        joined.persist(StorageLevel.MEMORY_AND_DISK());

        for(String m :modes){
            switch (m){
                case "existence":
                    existence(groupTimes, support, metadata.getTraces());
                    break;
                case "absence":
                    absence(groupTimes, support, metadata.getTraces());
                    break;
                case "exactly":
                    exactly(groupTimes, support, metadata.getTraces());
                    break;
                case "co-existence":
                    coExistence(joined, bUniqueSingle, bSupport, bTotalTraces);
                    break;
                case "choice":
                    choice(uEventType, bSupport, bTotalTraces);
                    break;
                case "exclusive-choice":
                    exclusiveChoice(uEventType, bUniqueSingle, bSupport, bTotalTraces);
                    break;
                case "responded-existence":
                    respondedExistence(joined, bUniqueSingle, bSupport, bTotalTraces);
                    break;
            }

        }

        // unpersist whetever needed
        joined.unpersist();
        uPairs.unpersist();
        uEventType.unpersist();
        return this.queryResponseExistence;
    }

    public List<String> evaluateModes(List<String> modes) {
        List<String> s = new ArrayList<>();
        List<String> l = Arrays.asList("existence", "absence", "exactly", "co-existence", "choice",
                "exclusive-choice", "responded-existence");
        Set<String> correct_ms = new HashSet<>(l);
        for(String m : modes){
            if(!correct_ms.contains(m)){
                s.add(m);
            }
        }
        return s;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    private Map<String, HashMap<Integer, Long>> createMapForSingle(JavaRDD<UniqueTracesPerEventType> uEventType) {
        Map<String, HashMap<Integer, Long>> groupTimes = uEventType.map(x -> new Tuple2<>(x.getEventType(), x.groupTimes()))
                .keyBy(x -> x._1)
                .mapValues(x -> x._2)
                .collectAsMap();
        return groupTimes;
    }

    private Map<String, Long> extractUniqueTracesSingle(Map<String, HashMap<Integer, Long>> groupTimes) {
        Map<String, Long> response = new HashMap<>();
        groupTimes.entrySet().stream().map(x -> {
            long s = x.getValue().values().stream().mapToLong(l -> l).sum();
            return new Tuple2<>(x.getKey(), s);
        }).forEach(x -> response.put(x._1, x._2));
        return response;
    }

    private JavaRDD<Tuple3<String, String, Integer>> joinUnionTraces(JavaRDD<UniqueTracesPerEventPair> uPairs) {
        return uPairs
                .keyBy(UniqueTracesPerEventPair::getKey)
                .leftOuterJoin(uPairs.keyBy(UniqueTracesPerEventPair::getKeyReverse))
                .map(x -> {
                    UniqueTracesPerEventPair right = x._2._2.
                            orElse(new UniqueTracesPerEventPair(x._1._2, x._1._1, new ArrayList<>()));
                    // find union of the 2 lists
                    Set<Long> set = new LinkedHashSet<>(x._2._1.getUniqueTraces());
                    set.addAll(right.getUniqueTraces());
                    ArrayList<Long> combinedList = new ArrayList<>(set);
                    return new Tuple3<>(x._1._1, x._1._2, combinedList.size());
                });

    }


    private void existence(Map<String, HashMap<Integer, Long>> groupTimes, double support, long totalTraces) {
        Set<String> eventTypes = groupTimes.keySet();
        List<EventN> response = new ArrayList<>();
        for (String et : eventTypes) { //for every event type
            HashMap<Integer, Long> t = groupTimes.get(et);
            List<Integer> times = new ArrayList<>(t.keySet()).stream().sorted(Comparator.reverseOrder())
                    .collect(Collectors.toList());
            for (int time : times) {
                double s = (double) times.stream().filter(x -> x >= time).mapToLong(t::get).sum() / totalTraces;
                if (s >= support) {
                    response.add(new EventN(et, time, s));
                    break;
                }
            }
        }
        this.queryResponseExistence.setExistence(response);
    }

    private void absence(Map<String, HashMap<Integer, Long>> groupTimes, double support, long totalTraces) {
        Set<String> eventTypes = groupTimes.keySet();
        List<EventN> response = new ArrayList<>();
        for (String et : eventTypes) { //for every event type
            HashMap<Integer, Long> t = groupTimes.get(et);
            long totalSum = t.values().stream().mapToLong(x -> x).sum();
            t.put(0, totalTraces - totalSum);
            List<Integer> times = new ArrayList<>(t.keySet()).stream().sorted().collect(Collectors.toList());
            if (!times.contains(2)) times.add(2); //to be sure that it will run at least once
            for (int time : times) {
                if (time < 2) {
                    continue;
                }
                double s = (double) times.stream().filter(x -> x < time).mapToLong(t::get).sum() / totalTraces;
                if (s >= support) {
                    response.add(new EventN(et, time, s));
                    break;
                }
            }
        }
        this.queryResponseExistence.setAbsence(response);
    }

    private void exactly(Map<String, HashMap<Integer, Long>> groupTimes, double support, long totalTraces) {
        Set<String> eventTypes = groupTimes.keySet();
        List<EventN> response = new ArrayList<>();
        for (String et : eventTypes) { //for every event type
            HashMap<Integer, Long> t = groupTimes.get(et);
            long totalSum = t.values().stream().mapToLong(x -> x).sum();
            if (!t.containsKey(0)) t.put(0, totalTraces - totalSum);
            for (Map.Entry<Integer, Long> x : t.entrySet()) {
                if (x.getValue() >= (support * totalTraces)) {
                    response.add(new EventN(et, x.getKey(), x.getValue().doubleValue() / totalTraces));
                }
            }
        }
        this.queryResponseExistence.setExactly(response);
    }

    private void coExistence(JavaRDD<Tuple3<String, String, Integer>> joinedUnion,
                             Broadcast<Map<String, Long>> bUniqueSingle, Broadcast<Double> bSupport, Broadcast<Long> bTotalTraces) {
        //valid event types can be used as first in a pair (since they have support greater than the user-defined)

        List<EventPairSupport> notCoExistence = joinedUnion
                .filter(x -> x._1().compareTo(x._2()) <= 0)//filter same pair that appears in both ways
                .filter(x -> x._3() <= ((1 - bSupport.getValue()) * bTotalTraces.getValue()))
                .map(x -> new EventPairSupport(x._1(), x._2(), (double) x._3() / bTotalTraces.getValue()))
                .collect();
        queryResponseExistence.setNotCoExistence(notCoExistence);

        // |A| = |IndexTable(a,b) U IndexTable(b,a)|, i.e., unique traces where a and b co-exist
        // total_traces = |A| + (non-of them exist) + (only a exist) + (only b exist) (1)
        // (only a exists) = (unique traces of a) - (traces that a co-existed with b)
        // where the co-existence is true is when |A|+(non-of them exist) >= support (2)
        // (1)+(2)=> total_traces - (unique traces of a) + |A| - (unique traces of b) + |A| >= support* total_traces
        //  total_traces - (unique traces of a) - (unique traces of b) - |A| >= support* total_traces
        List<EventPairSupport> coExistence = joinedUnion
                .filter(x -> x._1().compareTo(x._2()) <= 0)
                .filter(x -> x._3() >= (bSupport.getValue()) * bTotalTraces.getValue())
                .map(x -> {
                    double sup = (bTotalTraces.getValue() - bUniqueSingle.getValue().get(x._1()) -
                            bUniqueSingle.getValue().get(x._2()) + 2L * x._3());
                    return new Tuple4<>(x._1(), x._2(), x._3(), sup);
                })
                .filter(x -> x._4() >= (bSupport.getValue() * bTotalTraces.getValue()))
                .collect()
                .stream().map(x -> new EventPairSupport(x._1(), x._2(), (double) x._4() / bTotalTraces.value()))
                .collect(Collectors.toList());
        queryResponseExistence.setCoExistence(coExistence);
    }

    private void choice(JavaRDD<UniqueTracesPerEventType> uEventType, Broadcast<Double> bSupport, Broadcast<Long> bTotalTraces) {
        //create possible pairs without duplication
        List<EventPairSupport> choice = uEventType.keyBy(UniqueTracesPerEventType::getEventType)
                .cartesian(uEventType.keyBy(UniqueTracesPerEventType::getEventType))
                .filter(x -> x._1._1.compareTo(x._2._1) < 0) //remove duplicate pairs
                .filter(x -> x._1._2.getOccurrences().size() + x._2._2.getOccurrences().size() >=
                        (bSupport.getValue()) * bTotalTraces.getValue())
                .map(x -> {
                    LinkedHashSet<Long> listA = x._1._2.getOccurrences().stream()
                            .map(y -> y._1).collect(Collectors.toCollection(LinkedHashSet::new));
                    listA.addAll(x._2._2.getOccurrences().stream().map(y -> y._1).collect(Collectors.toList()));
                    return new EventPairSupport(x._1._1, x._2._1, (double) listA.size() / bTotalTraces.getValue());
                })
                .filter(x -> x.getSupport() >= bSupport.getValue())
                .collect();
        queryResponseExistence.setChoice(choice);
    }

    private void exclusiveChoice(JavaRDD<UniqueTracesPerEventType> uEventType, Broadcast<Map<String, Long>> bUniqueSingle,
                                 Broadcast<Double> bSupport, Broadcast<Long> bTotalTraces) {
        List<EventPairSupport> exclusiveChoice = uEventType.keyBy(UniqueTracesPerEventType::getEventType)
                .cartesian(uEventType.keyBy(UniqueTracesPerEventType::getEventType))
                .filter(x -> x._1._1.compareTo(x._2._1) < 0) //remove duplicate pairs
                .filter(x -> x._1._2.getOccurrences().size() + x._2._2.getOccurrences().size() >=
                        (bSupport.getValue()) * bTotalTraces.getValue())
                .map(x -> {
                    LinkedHashSet<Long> listA = x._1._2.getOccurrences().stream()
                            .map(y -> y._1).collect(Collectors.toCollection(LinkedHashSet::new));
                    listA.retainAll(x._2._2.getOccurrences().stream().map(y -> y._1).collect(Collectors.toList()));
                    return new Tuple3<>(x._1._1, x._2._1, listA.size());
                })
                .map(x -> {
                    long s = (bUniqueSingle.getValue().get(x._1()) + bUniqueSingle.getValue().get(x._2()) - 2L * x._3());
                    double supp = (double) s / bTotalTraces.getValue();
                    return new EventPairSupport(x._1(), x._2(), supp);
                })
                .filter(x -> x.getSupport() >= bSupport.getValue())
                .collect();
        queryResponseExistence.setExclusiveChoice(exclusiveChoice);
    }

    private void respondedExistence(JavaRDD<Tuple3<String, String, Integer>> joined, Broadcast<Map<String, Long>> bUniqueSingle,
                                    Broadcast<Double> bSupport, Broadcast<Long> bTotalTraces) {
        List<EventPairSupport> responseExistence = joined.map(x -> {
                    double sup = ((double) x._3() + bTotalTraces.getValue() - bUniqueSingle.getValue().get(x._1())) / bTotalTraces.getValue();
                    return new EventPairSupport(x._1(), x._2(), sup);
                })
                .filter(x -> x.getSupport() >= bSupport.getValue())
                .collect();
        this.queryResponseExistence.setRespondedExistence(responseExistence);

    }


}
