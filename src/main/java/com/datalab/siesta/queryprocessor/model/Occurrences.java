package com.datalab.siesta.queryprocessor.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * The total occurrences of a single trace. Since each trace can have multiple occurrences of the query pattern,
 * this class groups them together so they can be easier represented in the response
 */
public class Occurrences {

    protected String traceID;

    protected List<Occurrence> occurrences;

    public Occurrences(String traceID, List<Occurrence> occurrences) {
        this.traceID = traceID;
        this.occurrences = occurrences;
    }

    @JsonIgnore
    public boolean isEmpty(){
        return this.occurrences.isEmpty();
    }

    public Occurrences() {
        this.occurrences = new ArrayList<>();
    }

    public String getTraceID() {
        return traceID;
    }

    public void setTraceID(String traceID) {
        this.traceID = traceID;
    }

    public void addOccurrence(Occurrence oc) {
        this.occurrences.add(oc);
    }

    public List<Occurrence> getOccurrences() {
        return occurrences;
    }

    public void setOccurrences(List<Occurrence> occurrences) {
        this.occurrences = occurrences;
    }

    /**
     * Determines which of the found occurrences will be returned. This depends on the parameter returnAll, the
     * number of events in each occurrence and if the various occurrences overlap in time
     * @param returnAll query parameter that determines if all of non-overlapping occurrences will be returned
     */
    public void clearOccurrences(boolean returnAll) { //here we can determine different selection policies
        List<Occurrence> response = new ArrayList<>() {
            {
                Occurrence e = occurrences.get(0);
                if (occurrences.size()>1){
                    for (int i=1;i<occurrences.size();i++){ //add the occurrence with the largest size
                        if(occurrences.get(i).getOccurrence().size()>e.getOccurrence().size()){
                            e=occurrences.get(i);
                        }
                    }
                }
                add(e);
            }
        };
        if (!returnAll) { //return the one occurrence with the largest size (that is if Kleene* or Kleene+ was used)
            this.occurrences = response;
            return;
        } else {
            //return all occurrences as long as they do not overlap
            for (int i = 1; i < occurrences.size(); i++) {
                boolean overlaps = false;
                for (Occurrence o : response) {
                    if (occurrences.get(i).overlaps(o)) {
                        overlaps = true;
                        break;
                    }
                }
                if (!overlaps) response.add(occurrences.get(i));
            }
        }
        this.occurrences = response;
    }
}
