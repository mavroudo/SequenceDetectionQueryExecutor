package com.datalab.siesta.queryprocessor.declare.model;

import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

public class UniqueTracesPerEventType implements Serializable {
    private String eventType;
    private List<Tuple2<Long,Integer>> occurrences;

    public UniqueTracesPerEventType() {
    }

    public UniqueTracesPerEventType(String eventType, List<Tuple2<Long, Integer>> occurrences) {
        this.eventType = eventType;
        this.occurrences = occurrences;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public List<Tuple2<Long, Integer>> getOccurrences() {
        return occurrences;
    }

    public void setOccurrences(List<Tuple2<Long, Integer>> occurrences) {
        this.occurrences = occurrences;
    }

    public HashMap<Integer,Long> groupTimes(){
        HashMap<Integer,Long> groups = new HashMap<>();
        this.occurrences.forEach(x->{
            if(groups.containsKey(x._2)){
                groups.put(x._2,groups.get(x._2)+1L);
            }else{
                groups.put(x._2,1L);
            }
        });
        return groups;
    }
}
