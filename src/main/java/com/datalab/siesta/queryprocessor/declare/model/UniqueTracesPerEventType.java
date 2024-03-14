package com.datalab.siesta.queryprocessor.declare.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

/**
 * These data are extracted from the SingleTable. This class contains how many occurrences each distinct trace
 * has
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class UniqueTracesPerEventType implements Serializable {
    private String eventType;
    private List<OccurrencesPerTrace> occurrences;


    /**
     * Extract the list of TraceId-Occurrences as a map where the traces are grouped based on the number
     * of occurrences for this particular event types. It is used in the absence and existence templates
     * @return a map Occurrences -> trace ids based on the list of TraceId-Occurrences
     */
    public HashMap<Integer,Long> groupTimes(){
        HashMap<Integer,Long> groups = new HashMap<>();
        this.occurrences.forEach(x->{
            if(groups.containsKey(x.getOccurrences())){
                groups.put(x.getOccurrences(),groups.get(x.getOccurrences())+1L);
            }else{
                groups.put(x.getOccurrences(),1L);
            }
        });
        return groups;
    }
}
