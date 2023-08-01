package com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations;

import org.springframework.stereotype.Service;
import scala.Tuple4;
import scala.Tuple5;

import java.io.Serializable;
import java.util.Set;

@Service
public class OrderedRelationsUtilityFunctions implements Serializable {

    public Tuple4<String, String, String, Integer> countResponse(Tuple5<String, String, Long, Set<Integer>, Set<Integer>> line) {
        int s = 0;
        for (int a : line._4()) {
            if (line._5().stream().anyMatch(y -> y > a)) s += 1;
        }
        return new Tuple4<>(line._1(), line._2(), "r", s);
    }

    public Tuple4<String, String, String, Integer> countPrecedence(Tuple5<String, String, Long, Set<Integer>, Set<Integer>> line) {
        int s = 0;
        for (int a : line._5()) {
            if (line._4().stream().anyMatch(y -> y < a)) s += 1;
        }
        return new Tuple4<>(line._1(), line._2(), "p", s);
    }
}
