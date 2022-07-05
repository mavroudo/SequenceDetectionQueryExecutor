package com.sequence.detection.rest.util;

import com.sequence.detection.rest.model.Event;
import com.sequence.detection.rest.model.QueryPair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SetCover {

    public static List<QueryPair> findSetCover(List<QueryPair> queryPairs, Map<QueryPair, Integer> costs, Set<Event> universe) {
        Set<QueryPair> results = new HashSet<>();
        Set<Event> I = new HashSet<>();
        Set<Event> temp;
        while (!universe.equals(I) && results.size() != queryPairs.size()) {
            QueryPair q = null;
            int cost = Integer.MAX_VALUE;
            for (QueryPair qp : queryPairs) {
                if (!results.contains(qp)) {
                    temp = Stream.concat(I.stream(), qp.getEvents().stream()).collect(Collectors.toSet());
                    int newElements = temp.size() - I.size();
                    if (newElements > 0) {
                        int c = costs.get(qp) / newElements;
                        if (c < cost) {
                            cost = c;
                            q = qp;
                        }
                    }
                }
            }
            if (q != null) {
                results.add(q);
                I.addAll(q.getEvents());
            }else{
                break;
            }

        }
        return new ArrayList<>(results);
    }
}
