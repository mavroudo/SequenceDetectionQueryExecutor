package com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations;

import com.datalab.siesta.queryprocessor.declare.model.Abstract2OrderConstraint;
import com.datalab.siesta.queryprocessor.declare.model.EventPairTraceOccurrences;
import org.springframework.stereotype.Service;


import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Contain functions that are utilized to count the occurrences of order-relation constraints.
 * It is required two list of integers (one for each event type) which contains the positions of
 * the occurrences within this trace
 */
@Service
public class OrderedRelationsUtilityFunctions implements Serializable {

    /**
     * Counts the occurrences for the 'response(eventA,eventB)' constraint
     * @param line a {@link EventPairTraceOccurrences} object
     * @return # of occurrences for the 'response(eventA,eventB)' constraint
     */
    public Abstract2OrderConstraint countResponse(EventPairTraceOccurrences line) {
        int s = 0;
        for (int a : line.getOccurrencesA()) {
            if (line.getOccurrencesB().stream().anyMatch(y -> y > a)) s += 1;
        }
        return new Abstract2OrderConstraint(line.getEventA(), line.getEventB(), "r", s);
    }

    /**
     * Counts the occurrences for the 'precedence(eventA,eventB)' constraint
     * @param line a {@link EventPairTraceOccurrences} object
     * @return # of occurrences for the 'response(eventA,eventB)' constraint
     */
    public Abstract2OrderConstraint countPrecedence(EventPairTraceOccurrences line) {
        int s = 0;
        for (int a : line.getOccurrencesB()) {
            if (line.getOccurrencesA().stream().anyMatch(y -> y < a)) s += 1;
        }
        return new Abstract2OrderConstraint(line.getEventA(), line.getEventB(), "p", s);
    }

    /**
     * Counts the occurrences for the 'alternate response(eventA,eventB)' constraint
     * @param line a {@link EventPairTraceOccurrences} object
     * @return # of occurrences for the 'response(eventA,eventB)' constraint
     */
    public Abstract2OrderConstraint countResponseAlternate(EventPairTraceOccurrences line) {
        int s = 0;
        List<Integer> aList = line.getOccurrencesA().stream().sorted().collect(Collectors.toList());
        for (int i = 0; i < aList.size() - 1; i++) {
            int finalI = i;
            if(line.getOccurrencesB().stream().anyMatch(y-> y>aList.get(finalI) && y<aList.get(finalI +1))) s+=1;
        }
        if(line.getOccurrencesB().stream().anyMatch(y-> y>aList.get(aList.size()-1))) s+=1;
        return new Abstract2OrderConstraint(line.getEventA(), line.getEventB(), "r", s);
    }

    /**
     * Counts the occurrences for the 'alternate precedence(eventA,eventB)' constraint
     * @param line a {@link EventPairTraceOccurrences} object
     * @return # of occurrences for the 'response(eventA,eventB)' constraint
     */
    public Abstract2OrderConstraint countPrecedenceAlternate(EventPairTraceOccurrences line) {
        int s = 0;
        List<Integer> bList = line.getOccurrencesB().stream().sorted().collect(Collectors.toList());
        for (int i = 1; i < bList.size() ; i++) {
            int finalI = i;
            if(line.getOccurrencesA().stream().anyMatch(y-> y<bList.get(finalI) && y>bList.get(finalI -1))) s+=1;
        }
        if(line.getOccurrencesA().stream().anyMatch(y-> y<bList.get(0))) s+=1;
        return new Abstract2OrderConstraint(line.getEventA(), line.getEventB(), "p", s);
    }

    /**
     * Counts the occurrences for the 'chain response(eventA,eventB)' constraint
     * @param line a {@link EventPairTraceOccurrences} object
     * @return # of occurrences for the 'response(eventA,eventB)' constraint
     */
    public Abstract2OrderConstraint countResponseChain(EventPairTraceOccurrences line) {
        int s = 0;
        for (int a : line.getOccurrencesA()) {
            if (line.getOccurrencesB().stream().anyMatch(y -> y == a+1)) s += 1;
        }
        return new Abstract2OrderConstraint(line.getEventA(), line.getEventB(), "r", s);
    }

    /**
     * Counts the occurrences for the 'chain precedence(eventA,eventB)' constraint
     * @param line a {@link EventPairTraceOccurrences} object
     * @return # of occurrences for the 'response(eventA,eventB)' constraint
     */
    public Abstract2OrderConstraint countPrecedenceChain(EventPairTraceOccurrences line) {
        int s = 0;
        for (int a : line.getOccurrencesB()) {
            if (line.getOccurrencesA().stream().anyMatch(y -> y == a-1)) s += 1;
        }
        return new Abstract2OrderConstraint(line.getEventA(), line.getEventB(), "p", s);
    }


}
