package com.datalab.siesta.queryprocessor.declare;

import com.datalab.siesta.queryprocessor.declare.model.EventPairToNumberOfTrace;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import org.apache.spark.api.java.JavaRDD;
import org.jvnet.hk2.annotations.Service;
import org.springframework.stereotype.Component;

import java.util.*;

@Service
@Component
public class DeclareUtilities {

    public DeclareUtilities() {
    }

    /**
     * Extract the pairs not found in the database
     * @param eventTypes a list of all the event types
     * @param joined a rdd containing all the event pairs that occurred
     * @return a set of all the event pairs that did not appear in the log database
     */
    public Set<EventPair> extractNotFoundPairs(Set<String> eventTypes,JavaRDD<EventPairToNumberOfTrace> joined) {

        //calculate all the event pairs (n^2) and store them in a set
        //event pairs of type (eventA,eventA) are excluded
        Set<EventPair> allEventPairs = new HashSet<>();
        for (String eventA : eventTypes) {
            for (String eventB : eventTypes) {
                if (!eventA.equals(eventB)) {
                    allEventPairs.add(new EventPair(new Event(eventA), new Event(eventB)));
                }
            }
        }
        //removes from the above set all the event pairs that have at least one occurrence in the
        List<EventPair> foundEventPairs = joined.map(x -> new EventPair(new Event(x.getEventA()), new Event(x.getEventB())))
                .collect();
        foundEventPairs.forEach(allEventPairs::remove);
        return allEventPairs;
    }
}
