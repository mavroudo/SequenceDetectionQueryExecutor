package com.datalab.siesta.queryprocessor.model.WhyNotMatch;

import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Events.EventTs;
import com.datalab.siesta.queryprocessor.model.WhyNotMatch.UsingSase.UncertainTimeEvent;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.sql.Timestamp;
import java.util.List;

/**
 * Contains a match that was found in the modified events. It contains the match found, the original events and
 * the trace id.
 */
public class AlmostMatch {

    private long trace_id;

    private List<Event> original;

    private List<UncertainTimeEvent> match;

    private int totalChange;

    public AlmostMatch(long trace_id, List<Event> original, List<UncertainTimeEvent> match) {
        this.trace_id = trace_id;
        this.original = original;
        this.match = match;
        totalChange = this.match.stream().mapToInt(UncertainTimeEvent::getChange).sum();
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

    /**
     * It constructs a string that describes the modification that is required in the original events in order to
     * appear an occurrence of the query pattern. There is also information about the total amount of modifications needed.
     *
     * @return a string that describes the required modification in the original events in order for a pattern
     * occurrence to appear
     */
    @JsonIgnore
    public String getRecommendation() {
        StringBuilder sb = new StringBuilder("This would be a match if:\n");
        for (int i = 0; i < this.match.size(); i++) {
            UncertainTimeEvent ut = this.match.get(i);
            if (ut.getChange() != 0) { //only for those that changed
                Event e = this.getCorrespondingEvent(ut,i);
                if (e instanceof EventTs) { //changes in timestamp
                    EventTs et = (EventTs) e;
                    sb.append(String.format(" event %s timestamp was changed from %s to %s\n", et.getName(), et.getTimestamp(),
                            new Timestamp(ut.getTimestamp() * 1000L)));
                } else if (e instanceof EventPos) { //changes in positions
                    EventPos et = (EventPos) e;
                    sb.append(String.format(" event %s position was changed from %d to %d\n", et.getName(), et.getPosition(),
                            ut.getPosition()));
                }
            }

        }
        sb.append(String.format("total modification: %s", this.convertSeconds(totalChange)));
        return sb.toString();
    }

    private Event getCorrespondingEvent(UncertainTimeEvent ut, int pos) {
        for (int i = 0; i < this.original.size(); i++) {
            Event e = this.original.get(i);
            if (!e.getName().equals(ut.getEventType())) { // only care for similar
                continue;
            }
            if (e instanceof EventTs) {
                EventTs et = (EventTs) e;
                if (Math.abs(et.getTimestamp().getTime() - new Timestamp(ut.getTimestamp() * 1000L).getTime())/1000 == ut.getChange()) {
                    return et;
                }
            } else {
                EventPos ep = (EventPos) e;
                if (Math.abs(ep.getPosition() - ut.getPosition()) == ut.getChange()) {
                    return ep;
                }
            }
        }
        return this.original.get(pos);
    }

    private String convertSeconds(int totalSeconds) {
        int hours = totalSeconds / 3600;
        int minutes = (totalSeconds % 3600) / 60;
        int seconds = totalSeconds % 60;

        if (hours > 0) {
            return hours + " hours " + minutes + " minutes " + seconds + " seconds";
        } else if (minutes > 0) {
            return minutes + " minutes " + seconds + " seconds";
        } else {
            return seconds + " seconds";
        }
    }

}
