package com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations;

import org.springframework.stereotype.Service;
import scala.Tuple4;
import scala.Tuple5;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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

    public Tuple4<String, String, String, Integer> countResponseAlternate
            (Tuple5<String, String, Long, Set<Integer>, Set<Integer>> line) {
        int s = 0;
        List<Integer> aList = line._4().stream().sorted().collect(Collectors.toList());
        for (int i = 0; i < aList.size() - 1; i++) {
            int finalI = i;
            if(line._5().stream().anyMatch(y-> y>aList.get(finalI) && y<aList.get(finalI +1))) s+=1;
        }
        if(line._5().stream().anyMatch(y-> y>aList.get(aList.size()-1))) s+=1;
        return new Tuple4<>(line._1(), line._2(), "r", s);
    }

    public Tuple4<String, String, String, Integer> countPrecedenceAlternate
            (Tuple5<String, String, Long, Set<Integer>, Set<Integer>> line) {
        int s = 0;
        List<Integer> bList = line._5().stream().sorted().collect(Collectors.toList());
        for (int i = 1; i < bList.size() ; i++) {
            int finalI = i;
            if(line._4().stream().anyMatch(y-> y<bList.get(finalI) && y>bList.get(finalI -1))) s+=1;
        }
        if(line._4().stream().anyMatch(y-> y<bList.get(0))) s+=1;
        return new Tuple4<>(line._1(), line._2(), "p", s);
    }

    public Tuple4<String, String, String, Integer> countResponseChain(Tuple5<String, String, Long, Set<Integer>, Set<Integer>> line) {
        int s = 0;
        for (int a : line._4()) {
            if (line._5().stream().anyMatch(y -> y == a+1)) s += 1;
        }
        return new Tuple4<>(line._1(), line._2(), "r", s);
    }

    public Tuple4<String, String, String, Integer> countPrecedenceChain(Tuple5<String, String, Long, Set<Integer>, Set<Integer>> line) {
        int s = 0;
        for (int a : line._5()) {
            if (line._4().stream().anyMatch(y -> y == a-1)) s += 1;
        }
        return new Tuple4<>(line._1(), line._2(), "p", s);
    }

}
