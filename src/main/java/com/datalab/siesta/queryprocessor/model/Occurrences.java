package com.datalab.siesta.queryprocessor.model;

import java.util.ArrayList;
import java.util.List;

public class Occurrences {

    private long traceID;

    private List<Occurrence> occurrences;

    public Occurrences(long traceID, List<Occurrence> occurrences) {
        this.traceID = traceID;
        this.occurrences = occurrences;
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
                add(occurrences.get(0));
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
