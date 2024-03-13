package com.datalab.siesta.queryprocessor.declare;

import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import org.apache.spark.api.java.JavaRDD;
import org.jvnet.hk2.annotations.Service;
import org.springframework.stereotype.Component;
import scala.Tuple3;

import java.util.*;

@Service
@Component
public class DeclareUtilities {

    public DeclareUtilities() {
    }

    /**
     * Extract the pairs not found in the database
     * @param groupTimes
     * @param joined
     * @return
     */
    public Set<EventPair> extractNotFoundPairs(Map<String, HashMap<Integer, Long>> groupTimes,
                                                  JavaRDD<Tuple3<String, String, Integer>> joined) {

        Set<EventPair> allEventPairs = new HashSet<>();
        for (String eventA : groupTimes.keySet()) {
            for (String eventB : groupTimes.keySet()) {
                if (!eventA.equals(eventB)) {
                    allEventPairs.add(new EventPair(new Event(eventA), new Event(eventB)));
                }
            }
        }
        List<EventPair> foundEventPairs = joined.map(x -> new EventPair(new Event(x._1()), new Event(x._2()))).collect();
        foundEventPairs.forEach(allEventPairs::remove);
        return allEventPairs;
    }
}
