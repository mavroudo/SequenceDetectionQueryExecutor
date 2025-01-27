package com.datalab.siesta.queryprocessor.declare.queryPlans.existence;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.DeclareUtilities;
import com.datalab.siesta.queryprocessor.declare.model.*;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseExistence;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryExistenceWrapper;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;

import lombok.Setter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

@Component
@RequestScope
public class QueryPlanExistences implements QueryPlan{

    private final DeclareDBConnector declareDBConnector;
    private final JavaSparkContext javaSparkContext;
    //initialize a protected variable of the required events
    private QueryResponseExistence queryResponseExistence;
    @Setter
    private Metadata metadata;

    private final DeclareUtilities declareUtilities;


    @Autowired
    public QueryPlanExistences(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext,
                               DeclareUtilities declareUtilities) {
        this.declareDBConnector = declareDBConnector;
        this.javaSparkContext = javaSparkContext;
        this.queryResponseExistence = new QueryResponseExistence();
        this.declareUtilities = declareUtilities;
    }

    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryExistenceWrapper qew = (QueryExistenceWrapper) qw;
        Broadcast<Double> bSupport = javaSparkContext.broadcast(qew.getSupport());
        Broadcast<Long> bTotalTraces = javaSparkContext.broadcast(metadata.getTraces());

        //if existence, absence or exactly in modes
        JavaRDD<UniqueTracesPerEventType> uEventType = declareDBConnector.querySingleTableDeclare(metadata.getLogname());
        uEventType.persist(StorageLevel.MEMORY_AND_DISK());
        Map<String, HashMap<Integer, Long>> groupTimes = this.createMapForSingle(uEventType);
        Map<String, Long> singleUnique = this.extractUniqueTracesSingle(groupTimes);
        Broadcast<Map<String, Long>> bUniqueSingle = javaSparkContext.broadcast(singleUnique);

        JavaRDD<UniqueTracesPerEventPair> uPairs = declareDBConnector.queryIndexTableDeclare(metadata.getLogname());
        uPairs.persist(StorageLevel.MEMORY_AND_DISK());
        JavaRDD<EventPairToNumberOfTrace> joined = joinUnionTraces(uPairs);
        joined.persist(StorageLevel.MEMORY_AND_DISK());

        Set<EventPair> notFoundPairs = declareUtilities.extractNotFoundPairs(groupTimes.keySet(),joined);

        for (String m : qew.getModes()) {
            switch (m) {
                case "existence":
                    existence(groupTimes, qew.getSupport(), metadata.getTraces());
                    break;
                case "absence":
                    absence(groupTimes, qew.getSupport(), metadata.getTraces());
                    break;
                case "exactly":
                    exactly(groupTimes, qew.getSupport(), metadata.getTraces());
                    break;
                case "co-existence":
                    coExistence(joined, bUniqueSingle, bSupport, bTotalTraces,notFoundPairs);
                    break;
                case "not-co-existence":
                    notCoExistence(joined, bUniqueSingle, bSupport, bTotalTraces,notFoundPairs);
                    break;
                case "choice":
                    choice(uEventType, bSupport, bTotalTraces);
                    break;
                case "exclusive-choice":
                    exclusiveChoice(joined, bUniqueSingle, bSupport, bTotalTraces,notFoundPairs);
                    break;
                case "responded-existence":
                    respondedExistence(joined, bUniqueSingle, bSupport, bTotalTraces);
                    break;
            }

        }

        // unpersist whatever needed
        joined.unpersist();
        uPairs.unpersist();
        uEventType.unpersist();
        return this.queryResponseExistence;
    }

    public QueryResponseExistence runAll(Map<String, HashMap<Integer, Long>> groupTimes, double support,
                                         JavaRDD<EventPairToNumberOfTrace> joined, Broadcast<Double> bSupport,
                                         Broadcast<Long> bTotalTraces, Broadcast<Map<String, Long>> bUniqueSingle,
                                         JavaRDD<UniqueTracesPerEventType> uEventType) {
        Set<EventPair> notFoundPairs = declareUtilities.extractNotFoundPairs(groupTimes.keySet(),joined);

        //all event pairs will contain only those that weren't found in the dataset
        existence(groupTimes, support, metadata.getTraces());
        absence(groupTimes, support, metadata.getTraces());
        exactly(groupTimes, support, metadata.getTraces());
        coExistence(joined, bUniqueSingle, bSupport, bTotalTraces,notFoundPairs);
        choice(uEventType, bSupport, bTotalTraces);
        exclusiveChoice(joined, bUniqueSingle, bSupport, bTotalTraces,notFoundPairs);
        respondedExistence(joined, bUniqueSingle, bSupport, bTotalTraces);
        return this.queryResponseExistence;
    }

    /**
     * Extracts a map from a UniqueTracesPerEventType RDD. This map has the form
     * (Event Type) -> (number of occurrences) -> # of traces that contain that much amount of occurrences of this
     * event type. e.g. searching how many traces have exactly 2 instances of the event type 'c'
     * ('c')->(2) -> response
     * @param uEventType an RDD containing for each event type the unique traces and their corresponding occurrences
     *                   of this event type
     * @return a map of the form (Event Type) -> (number of occurrences) -> # of traces that contain that
     * much amount of occurrences of this event type.
     */
    public Map<String, HashMap<Integer, Long>> createMapForSingle(JavaRDD<UniqueTracesPerEventType> uEventType) {
        return uEventType
                .map(x -> new Tuple2<>(x.getEventType(), x.groupTimes()))
                .keyBy(x -> x._1)
                .mapValues(x -> x._2)
                .collectAsMap();
    }

    /**
     * Based on the output of the above function, this code extracts the number of traces that contain a particular
     * event type, i.e. the response will contain information (event type) -> #traces containing it
     * @param groupTimes a map of the form (Event Type) -> (number of occurrences) -> # of traces that contain that
     *      * much amount of occurrences of this event type.
     * @return a map in the form (event type) -> #traces containing it
     */
    public Map<String, Long> extractUniqueTracesSingle(Map<String, HashMap<Integer, Long>> groupTimes) {
        Map<String, Long> response = new HashMap<>();
        groupTimes.entrySet().stream().map(x -> {
            long s = x.getValue().values().stream().mapToLong(l -> l).sum();
            return new Tuple2<>(x.getKey(), s);
        }).forEach(x -> response.put(x._1, x._2));
        return response;
    }

    /**
     * @param uPairs information extracted from the index table
     * @return a rdd of the type (eventA, eventB, traceId), i.e., which traces contain a specific event pair
     */
    public JavaRDD<EventPairToNumberOfTrace> joinUnionTraces(JavaRDD<UniqueTracesPerEventPair> uPairs) {

        return uPairs
                .keyBy(UniqueTracesPerEventPair::getKey)
                .leftOuterJoin(uPairs.keyBy(UniqueTracesPerEventPair::getKeyReverse))
                .map(x -> {
                    UniqueTracesPerEventPair right = x._2._2.
                            orElse(new UniqueTracesPerEventPair(x._1._2, x._1._1, new ArrayList<>()));
                    // find union of the 2 lists
                    Set<String> set = new LinkedHashSet<>(x._2._1.getUniqueTraces());
                    set.addAll(right.getUniqueTraces());
                    ArrayList<String> combinedList = new ArrayList<>(set);
                    return new EventPairToNumberOfTrace(x._1._1, x._1._2, combinedList.size());
                });

    }

    /**
     * Extract the constraints that correspond to the 'existence' template.
     * @param groupTimes a map of the form (Event Type) -> (number of occurrences) -> # of traces that contain that
     *      * much amount of occurrences of this event type.
     * @param support minimum support that a pattern should have in order to be included in the result set
     * @param totalTraces the total number of traces in this log database
     */
    private void existence(Map<String, HashMap<Integer, Long>> groupTimes, double support, long totalTraces) {
        Set<String> eventTypes = groupTimes.keySet();
        List<EventN> response = new ArrayList<>();
        for (String et : eventTypes) { //for every event type
            HashMap<Integer, Long> t = groupTimes.get(et);
            List<Integer> times = new ArrayList<>(t.keySet()).stream().sorted(Comparator.reverseOrder())
                    .collect(Collectors.toList());
            for (int time=3;time>0;time--) {
                int finalTime = time;
                double s = (double) times.stream().filter(x -> x >= finalTime).mapToLong(t::get).sum() / totalTraces;
                if (s >= support) {
                    response.add(new EventN(et, time, s));
                }
            }
        }
        this.queryResponseExistence.setExistence(response);
    }

    /**
     * Extract the constraints that correspond to the 'absence' template.
     * @param groupTimes a map of the form (Event Type) -> (number of occurrences) -> # of traces that contain that
     *      * much amount of occurrences of this event type.
     * @param support minimum support that a pattern should have in order to be included in the result set
     * @param totalTraces the total number of traces in this log database
     */
    private void absence(Map<String, HashMap<Integer, Long>> groupTimes, double support, long totalTraces) {
        Set<String> eventTypes = groupTimes.keySet();
        List<EventN> response = new ArrayList<>();
        for (String et : eventTypes) { //for every event type
            HashMap<Integer, Long> t = groupTimes.get(et);
            long totalSum = t.values().stream().mapToLong(x -> x).sum();
            t.put(0, totalTraces - totalSum);
            List<Integer> times = new ArrayList<>(t.keySet()).stream().sorted().collect(Collectors.toList());
            if (!times.contains(2)) times.add(2); //to be sure that it will run at least once
            for (int time=3;time>=2;time--) {
                int finalTime = time;
                double s = (double) times.stream().filter(x -> x < finalTime).map(t::get)
                        .filter(Objects::nonNull).mapToLong(x -> x).sum() / totalTraces;
                if (s >= support) {
                    response.add(new EventN(et, time, s));
                }
            }
        }
        this.queryResponseExistence.setAbsence(response);
    }

    /**
     * Extract the constraints that correspond to the 'exactly' template.
     * @param groupTimes a map of the form (Event Type) -> (number of occurrences) -> # of traces that contain that
     *      * much amount of occurrences of this event type.
     * @param support minimum support that a pattern should have in order to be included in the result set
     * @param totalTraces the total number of traces in this log database
     */
    private void exactly(Map<String, HashMap<Integer, Long>> groupTimes, double support, long totalTraces) {
        Set<String> eventTypes = groupTimes.keySet();
        List<EventN> response = new ArrayList<>();
        for (String et : eventTypes) { //for every event type
            HashMap<Integer, Long> t = groupTimes.get(et);
            long totalSum = t.values().stream().mapToLong(x -> x).sum();
            if (!t.containsKey(0)) t.put(0, totalTraces - totalSum);
            for (Map.Entry<Integer, Long> x : t.entrySet()) {
                if (x.getValue() >= (support * totalTraces) && x.getKey()>0) {
                    response.add(new EventN(et, x.getKey(), x.getValue().doubleValue() / totalTraces));
                }
            }
        }
        this.queryResponseExistence.setExactly(response);
    }

    /**
     * Extract the constraints that correspond to the 'exactly' template.
     * @param joinedUnion A RDD that contains objects of the form (eventA,eventB,traceID), i.e. which traces
     *                    contain at least one occurrence of the pair (eventA, eventB)
     * @param bUniqueSingle A spark broadcast map, that contains a map of the form (event type) -> # traces
     *                      that contain this event type
     * @param bSupport A spark broadcast variable, that corresponds to the user-defined support
     * @param bTotalTraces Total traces in this log database
     * @param notFound A set of the event pairs that have 0 occurrence in the log database
     */
    private void coExistence(JavaRDD<EventPairToNumberOfTrace> joinedUnion,
                             Broadcast<Map<String, Long>> bUniqueSingle, Broadcast<Double> bSupport,
                             Broadcast<Long> bTotalTraces, Set<EventPair> notFound) {

        // |A| = |IndexTable(a,b) U IndexTable(b,a)|, i.e., unique traces where a and b co-exist
        // total_traces = |A| + (non-of them exist) + (only 'a' exist) + (only b exist) (1)
        // (only 'a' exists) = (unique traces of 'a') - (traces that a co-existed with b)
        // where the co-existence is true is when |A|+(non-of them exist) >= support (2)
        // (1)+(2)=> total_traces - (unique traces of a) + |A| - (unique traces of b) + |A| >= support* total_traces
        //  total_traces - (unique traces of a) - (unique traces of b) - |A| >= support* total_traces
        List<EventPairSupport> coExistence = joinedUnion
                .filter(x-> !x.getEventA().equals(x.getEventB())) //remove pairs with the same event type
                .filter(x -> x.getEventA().compareTo(x.getEventB()) <= 0)
                .filter(x -> x.getNumberOfTraces() >= (bSupport.getValue()) * bTotalTraces.getValue())
                .map(x -> {
                    double sup = (bTotalTraces.getValue() - bUniqueSingle.getValue().get(x.getEventA()) -
                            bUniqueSingle.getValue().get(x.getEventB()) + 2L * x.getNumberOfTraces());
                    return new Abstract2DeclareConstraint(x.getEventA(), x.getEventB(), x.getNumberOfTraces(), sup);
                })
                .filter(x -> x.getSupport() >= (bSupport.getValue() * bTotalTraces.getValue()))
                .collect()
                .stream().map(x -> new EventPairSupport(x.getEventA(), x.getEventB(),
                        x.getSupport() / bTotalTraces.value()))
                .collect(Collectors.toList());
        queryResponseExistence.setCoExistence(coExistence);
    }

    private void notCoExistence(JavaRDD<EventPairToNumberOfTrace> joinedUnion,
                             Broadcast<Map<String, Long>> bUniqueSingle, Broadcast<Double> bSupport,
                             Broadcast<Long> bTotalTraces, Set<EventPair> notFound) {
        //valid event types can be used as first in a pair (since they have support greater than the user-defined)
        List<EventPairSupport> notCoExistence = joinedUnion
                .filter(x-> !x.getEventA().equals(x.getEventB())) //remove pairs with the same event type
                .filter(x -> x.getEventA().compareTo(x.getEventB()) <= 0)//filter same pair that appears in both ways
                .filter(x -> x.getNumberOfTraces() <= ((1 - bSupport.getValue()) * bTotalTraces.getValue()))//filter based on support
                .map(x -> new EventPairSupport(x.getEventA(), x.getEventB(),
                        1-(double) x.getNumberOfTraces() / bTotalTraces.getValue()))
                .collect();

        //Add all the pairs in the notFound that their reverse is also in this set. Meaning that these two
        //events never co-exist in the entire database
        Set<EventPairSupport> notCoExist = new HashSet<>();
        for(EventPair ep:notFound){
            if(notFound.contains(new EventPair(new Event(ep.getEventB().getName()),new Event(ep.getEventA().getName())))){
                if(ep.getEventA().getName().compareTo(ep.getEventB().getName())>0) {
                    notCoExist.add(new EventPairSupport(ep.getEventA().getName(),ep.getEventB().getName(),1));
                }else{
                    notCoExist.add(new EventPairSupport(ep.getEventB().getName(),ep.getEventA().getName(),1));
                }
            }
        }
        // add all findings in one list
        List<EventPairSupport> response = new ArrayList<>();
        response.addAll(notCoExistence);
        response.addAll(notCoExist);
        queryResponseExistence.setNotCoExistence(response);
    }

    /**
     * Extract the constraints that correspond to the 'choice' template.
     * @param uEventType a RDD in the form (event type, [(traceId,#occurrences)])
     * @param bSupport A spark broadcast variable, that corresponds to the user-defined support
     * @param bTotalTraces Total traces in this log database
     */
    private void choice(JavaRDD<UniqueTracesPerEventType> uEventType, Broadcast<Double> bSupport, Broadcast<Long> bTotalTraces) {
        //create possible pairs without duplication
        List<EventPairSupport> choice = uEventType.keyBy(UniqueTracesPerEventType::getEventType)
                .cartesian(uEventType.keyBy(UniqueTracesPerEventType::getEventType))
                .filter(x -> x._1._1.compareTo(x._2._1) < 0) //remove duplicate pairs
                //filter based on the total number of traces that contain either of the two event types (early pruning)
                .filter(x -> x._1._2.getOccurrences().size() + x._2._2.getOccurrences().size() >=
                        (bSupport.getValue()) * bTotalTraces.getValue())
                //actual count the traces in which either of them exists (removing the duplicate counts - traces where
                //both exist)
                .map(x -> {
                    LinkedHashSet<String> listA = x._1._2.getOccurrences().stream()
                            .map(OccurrencesPerTrace::getTraceId).collect(Collectors.toCollection(LinkedHashSet::new));
                    listA.addAll(x._2._2.getOccurrences().stream().map(OccurrencesPerTrace::getTraceId)
                            .collect(Collectors.toList()));
                    return new EventPairSupport(x._1._1, x._2._1, (double) listA.size() / bTotalTraces.getValue());
                })
                //filter based on the support (correct filtering)
                .filter(x -> x.getSupport() >= bSupport.getValue())
                .collect();
        //add them to the result set
        queryResponseExistence.setChoice(choice);
    }


    /**
     * Extract the constraints that correspond to the 'exclusive choice' template.
     * @param joined A RDD that contains objects of the form (eventA,eventB,traceID), i.e. which traces
     *                    contain at least one occurrence of the pair (eventA, eventB)
     * @param bUniqueSingle A spark broadcast map, that contains a map of the form (event type) -> # traces
     *                      that contain this event type
     * @param bSupport A spark broadcast variable, that corresponds to the user-defined support
     * @param bTotalTraces Total traces in this log database
     * @param notFound A set of the event pairs that have 0 occurrence in the log database
     */
    private void exclusiveChoice(JavaRDD<EventPairToNumberOfTrace> joined, Broadcast<Map<String, Long>> bUniqueSingle,
                                 Broadcast<Double> bSupport, Broadcast<Long> bTotalTraces, Set<EventPair> notFound) {

        // detects exclusive choice in pairs that appear at least once in the log database
        List<EventPairSupport> exclusiveChoice = joined.filter(x -> x.getEventA().compareTo(x.getEventB()) < 0)
                .filter(x->!x.getEventA().equals(x.getEventB()))
                .map(x -> {
                    double sup = (double) (bUniqueSingle.getValue().get(x.getEventA())
                            + bUniqueSingle.getValue().get(x.getEventB()) - 2 * x.getNumberOfTraces()) / bTotalTraces.getValue();
                    return new EventPairSupport(x.getEventA(), x.getEventB(), sup);
                }).filter(x -> x.getSupport() >= bSupport.getValue())
                .collect();

        // detects exclusive choice in pairs that do not appear in the log database
        // therefore it checks if both (a,b) and (b,a) are in the notFound set
        Set<EventPairSupport> notCoExist = new HashSet<>();
        for(EventPair ep:notFound){
            if(notFound.contains(new EventPair(new Event(ep.getEventB().getName()),new Event(ep.getEventA().getName())))){
                //check the order of the names in order to add each pair only once
                if(ep.getEventA().getName().compareTo(ep.getEventB().getName())>0) {
                    notCoExist.add(new EventPairSupport(ep.getEventA().getName(),ep.getEventB().getName(),1));
                }else{
                    notCoExist.add(new EventPairSupport(ep.getEventB().getName(),ep.getEventA().getName(),1));
                }
            }
        }
        //Calculates the support of the constraints detected from the not found pairs, as this behavior
        //should describe at least 'support'% of the total traces
        List<EventPairSupport> exclusiveChoice2 = notCoExist.stream().map(x->{
            double sup = (double) (bUniqueSingle.getValue().get(x.getEventA()) +
                    bUniqueSingle.getValue().get(x.getEventB())) / bTotalTraces.getValue();
            return new EventPairSupport(x.getEventA(), x.getEventB(), sup);
        }).filter(x -> x.getSupport() >= bSupport.getValue())
                        .collect(Collectors.toList());

        //add both together and pass them to the response
        exclusiveChoice2.addAll(exclusiveChoice);
        queryResponseExistence.setExclusiveChoice(exclusiveChoice2);
    }

    /**
     * Extract the constraints that correspond to the 'exclusive choice' template.
     * @param joined A RDD that contains objects of the form (eventA,eventB,traceID), i.e. which traces
     *                    contain at least one occurrence of the pair (eventA, eventB)
     * @param bUniqueSingle A spark broadcast map, that contains a map of the form (event type) -> # traces
     *                      that contain this event type
     * @param bSupport A spark broadcast variable, that corresponds to the user-defined support
     * @param bTotalTraces Total traces in this log database
     */
    private void respondedExistence(JavaRDD<EventPairToNumberOfTrace> joined, Broadcast<Map<String, Long>> bUniqueSingle,
                                    Broadcast<Double> bSupport, Broadcast<Long> bTotalTraces) {
        List<EventPairSupport> responseExistence = joined
                .filter(x->!x.getEventA().equals(x.getEventB())) //remove duplicates, i.e. (eventA,eventA) pairs
                .flatMap((FlatMapFunction<EventPairToNumberOfTrace, EventPairSupport>)x->{
                    List<EventPairSupport> eps = new ArrayList<>();
                    //check support for the constraint responded-existence(a,b)
                    double sup = ((double) x.getNumberOfTraces() + bTotalTraces.getValue() -
                            bUniqueSingle.getValue().get(x.getEventA())) / bTotalTraces.getValue();
                    eps.add(new EventPairSupport(x.getEventA(), x.getEventB(), sup));
                    //check support for the constraint responded-existence(b,a)
                    sup = ((double) x.getNumberOfTraces() + bTotalTraces.getValue() -
                            bUniqueSingle.getValue().get(x.getEventB())) / bTotalTraces.getValue();
                    eps.add(new EventPairSupport(x.getEventB(), x.getEventA(), sup));
                    return eps.iterator();
                } )
                .distinct()
                .filter(x -> x.getSupport() >= bSupport.getValue())
                .collect();

        //pass to the response
        this.queryResponseExistence.setRespondedExistence(responseExistence);

    }

    public void initResponse() {
        this.queryResponseExistence = new QueryResponseExistence();
    }

}
