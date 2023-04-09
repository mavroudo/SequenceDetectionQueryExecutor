package com.datalab.siesta.queryprocessor.storage.repositories.CassandraRdd;

import com.datalab.siesta.queryprocessor.model.DBModel.IndexPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class IndexRow implements Serializable {

    private String event_a;
    private String event_b;
    private Timestamp start;
    private Timestamp end;
    private List<String> occurrences;

    public IndexRow() {
    }

    public IndexRow(String event_a, String event_b, Timestamp start, Timestamp end, List<String> occurrences) {
        this.event_a = event_a;
        this.event_b = event_b;
        this.start = start;
        this.end = end;
        this.occurrences = occurrences;
    }

    public boolean validate(Set<EventPair> pairs) {
        for (EventPair p : pairs) {
            if (p.getEventA().getName().equals(this.event_a) && p.getEventB().getName().equals(this.event_b))
                return true;
        }
        return false;
    }

    public List<IndexPair> extractOccurrences(Broadcast<String> mode, Broadcast<Timestamp> bfrom, Broadcast<Timestamp> btill) {
        List<IndexPair> indexPairs = new ArrayList<>();
        for (String trace : this.occurrences) {
            String[] split = trace.split("\\|\\|");
            long trace_id = Long.parseLong(split[0]);
            String[] p_split = split[1].split(",");
            for (String p : p_split) {
                String[] f = p.split("\\|");
                if (mode.value().equals("timestamps")) {
                    Timestamp tsA = Timestamp.valueOf(f[0]);
                    Timestamp tsB = Timestamp.valueOf(f[1]);
                    if (checkTS(bfrom, btill, tsA, tsB)) {
                        indexPairs.add(new IndexPair(trace_id, this.event_a, this.event_b, tsA, tsB));
                    }
                } else {
                    indexPairs.add(new IndexPair(trace_id, this.event_a, this.event_b, Integer.parseInt(f[0]),
                            Integer.parseInt(f[1])));
                }

            }
        }
        return indexPairs;
    }

    private boolean checkTS(Broadcast<Timestamp> bFrom, Broadcast<Timestamp> bTill, Timestamp tsA, Timestamp tsB) {
        if (bTill.value() != null && tsA.after(bTill.value())) return false;
        if (bFrom.value() != null && tsB.before(bFrom.value())) return false;

        //If from and till has been set we cannot check it here
        return true;
    }

    public String getEvent_a() {
        return event_a;
    }

    public void setEvent_a(String event_a) {
        this.event_a = event_a;
    }

    public String getEvent_b() {
        return event_b;
    }

    public void setEvent_b(String event_b) {
        this.event_b = event_b;
    }

    public Timestamp getStart() {
        return start;
    }

    public void setStart(Timestamp start) {
        this.start = start;
    }

    public Timestamp getEnd() {
        return end;
    }

    public void setEnd(Timestamp end) {
        this.end = end;
    }

    public List<String> getOccurrences() {
        return occurrences;
    }

    public void setOccurrences(List<String> occurrences) {
        this.occurrences = occurrences;
    }
}
