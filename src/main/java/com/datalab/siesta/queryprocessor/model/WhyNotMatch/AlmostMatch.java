package com.datalab.siesta.queryprocessor.model.WhyNotMatch;

import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Events.EventTs;
import com.datalab.siesta.queryprocessor.model.WhyNotMatch.UsingSase.UncertainTimeEvent;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.sql.Timestamp;
import java.util.List;

public class AlmostMatch {

    private long trace_id;

    private List<Event> original;

    private List<UncertainTimeEvent> match;

    private int totalChange;

    public AlmostMatch(long trace_id, List<Event> original, List<UncertainTimeEvent> match) {
        this.trace_id = trace_id;
        this.original = original;
        this.match = match;
        totalChange=this.match.stream().mapToInt(UncertainTimeEvent::getChange).sum();
    }

    public List<Event> getOriginal() {
        return original;
    }

    public long getTrace_id() {
        return trace_id;
    }

    public void setTrace_id(long trace_id) {
        this.trace_id = trace_id;
    }

    public void setOriginal(List<Event> original) {
        this.original = original;
    }

    public List<UncertainTimeEvent> getMatch() {
        return match;
    }

    public void setMatch(List<UncertainTimeEvent> match) {
        this.match = match;
    }

    @JsonIgnore
    public String getRecommendation() {
        StringBuilder sb = new StringBuilder("This would be a match if:\n");
        for (int i = 0; i < this.match.size(); i++) {
            UncertainTimeEvent ut = this.match.get(i);
            if (ut.getChange() != 0) { //only for those that changed
                if (this.original.get(i) instanceof EventTs) { //changes in timestamp
                    EventTs et = (EventTs) this.original.get(i);
                    sb.append(String.format(" event %s timestamp was changed from %s to %s\n", et.getName(), et.getTimestamp(),
                            new Timestamp(ut.getTimestamp() * 1000L)));
                } else if (this.original.get(i) instanceof EventPos) { //changes in positions
                    EventPos et = (EventPos) this.original.get(i);
                    sb.append(String.format(" event %s position was changed from %d to %d\n", et.getName(), et.getPosition(),
                            ut.getPosition()));
                }
            }

        }
        sb.append(String.format("total modification: %d",totalChange));
        return sb.toString();
    }
}
