package com.datalab.siesta.queryprocessor.model;

import org.codehaus.jackson.annotate.JsonIgnore;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Occurrences {

    protected long traceID;

    protected List<Occurrence> occurrences;

    public Occurrences(long traceID, List<Occurrence> occurrences) {
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

    public long getTraceID() {
        return traceID;
    }

    public void setTraceID(long traceID) {
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
        if (!returnAll) {
            this.occurrences = response;
            return;
        } else {
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
