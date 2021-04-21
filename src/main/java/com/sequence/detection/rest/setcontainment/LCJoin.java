package com.sequence.detection.rest.setcontainment;

import com.sequence.detection.rest.model.QueryPair;

import java.util.*;

public class LCJoin {


    public static List<Long> crossCuttingBasedIntersection( List<List<Long>> invertedLists) {
//        the lists are sorted small -> big and we want to find the intersection
        long maxid = 0;
        boolean endOfList = false;
        List<Long> containElement ;
        List<Long> greatestElement ;
        List<Long> results = new ArrayList<>();
        while (!endOfList) {
            containElement = new ArrayList<>();
            greatestElement = new ArrayList<>();
            for (List<Long> list : invertedLists) {
                long index = Arrays.binarySearch(list.toArray(), maxid);
                // logic
                long next_index;
                if (index >= 0) { // the element is located
                    containElement.add(maxid);
                    next_index=index+1;
                }else{
                    next_index=-(index+1);
                }
                if(next_index==list.size()){
                    endOfList=true;
                }else{
                    greatestElement.add(list.get((int) next_index));
                }
            }
            if(containElement.size() == invertedLists.size()){
                results.add(maxid);
            }
            if (!greatestElement.isEmpty()) {
                maxid = Collections.max(greatestElement);
            }
        }
        return results;

    }

}
