package com.sequence.detection.rest.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a sequence (funnel) of events
 *
 * @author Andreas Kosmatopoulos
 */
public class Sequence {
    /**
     * Events are stored in an ArrayList in the order they appear in the funnel
     */
    ArrayList<Event> seq; //Optimization: Use Deques for faster prepending

    public Sequence() {
        seq = new ArrayList<Event>();
    }

    public Sequence(Event... events) {
        seq = new ArrayList<Event>();
        seq.addAll(Arrays.asList(events));
    }

    public Sequence(Sequence other) {
        seq = new ArrayList<Event>();
        seq.addAll(other.seq);
    }

    /**
     * Insert the event at the end of the sequence
     *
     * @param event The event to be inserted
     */
    public void appendToSequence(Event event) {
        seq.add(event);
    }

    /**
     * Insert the event at the start of the sequence
     *
     * @param event The event to be inserted
     */
    public void prependToSequence(Event event) {
        seq.add(0, event);
    }

    public void removeLastEvent() {
        if (!seq.isEmpty())
            seq.remove(seq.size() - 1);
    }

    public void insert(Event event, int position) {
        seq.add(position, event);
    }

    public ArrayList<Event> getList() {
        return seq;
    }

    public Event getFirstEvent() {
        if (seq.isEmpty())
            return Event.EMPTY_EVENT;

        return seq.get(0);
    }

    public Event getLastEvent() {
        if (seq.isEmpty())
            return Event.EMPTY_EVENT;

        return seq.get(seq.size() - 1);
    }

    /**
     * Return a list of all query pairs for the funnel. For example, a funnel A-B-C has the following query pairs: (A,B), (B,C), (A,C)
     *
     * @return a list of all query pairs for the funnel
     */
    public List<QueryPair> getQueryTuples() {
        ArrayList<QueryPair> qpairs = new ArrayList<QueryPair>();

        int size = seq.size();
        if (size == 2)
            qpairs.add(new QueryPair(seq.get(0), seq.get(1)));

        Event first, second;
        if (size >= 3)
            for (int j = 0; j < size - 1; j++)
                for (int k = j + 1; k < size; k++) {
                    first = seq.get(j);
                    second = seq.get(k);
                    qpairs.add(new QueryPair(first, second));
                }

        return qpairs;
    }

    /**
     * Return a list of all query pairs for the funnel. For example, a funnel A-B-C has the following query pairs: (A,B), (B,C), (A,C)
     *
     * @return a list of all query pairs for the funnel
     */
    public List<QueryPair> getQueryTuplesConcequtive() {
        ArrayList<QueryPair> qpairs = new ArrayList<QueryPair>();

        int size = seq.size();
        if (size == 2)
            qpairs.add(new QueryPair(seq.get(0), seq.get(1)));

        Event first, second;
        if (size >= 3)
            for (int j = 0; j < size - 1; j++) {
                first = seq.get(j);
                second = seq.get(j + 1);
                qpairs.add(new QueryPair(first, second));
            }

        return qpairs;
    }

    @Override
    public String toString() {
        String strseq = "{";
        for (Event event : seq)
            strseq = strseq.concat(event + " --> ");
        strseq = strseq.substring(0, strseq.length() - 5);
        strseq = strseq.concat("}");

        return strseq;
    }

    public Event getEvent(int i) {
        try {
            return seq.get(i);
        } catch (IndexOutOfBoundsException e) {
            return Event.EMPTY_EVENT;
        }
    }

    public int getSize() {
        return seq.size();
    }

    /**
     * Check if this sequence exists in a (superset) sequence of events. Furthermore, the sequence must exist within a time limit
     *
     * @param allEvents   An ArrayList of timestamped events that correspond to all events by a particular device or user within a specified time frame
     * @param maxDuration The max duration time limit
     * @return an array of two values [timestamp of the first event occurrence, timestamp of the last event occurrence] in the list of timestamped events.
     * If this sequence is not contained in the greater list, [-1,-1] is returned
     */
    public long[] fitsTimestamps(List<TimestampedEvent> allEvents, long maxDuration) {
        ArrayList<TimestampedEvent> timestamps = new ArrayList<TimestampedEvent>();

        Date startDate = null;
        Date maxDate = null;
        Event currentEvent = seq.get(0);

        int i = 0;

        for (int j = 0; j < allEvents.size(); j++) {
            TimestampedEvent te = allEvents.get(j);

            if (maxDate != null && te.timestamp.after(maxDate)) {
                startDate = null;
                maxDate = null;
                j = j - 1;
                continue;
            }

            if (currentEvent.matches(te.event)) {
                if (startDate == null) {
                    startDate = te.timestamp;
                    maxDate = new Date(startDate.getTime() + maxDuration);
                }
                timestamps.add(te);
                i++;
                if (i == seq.size())
                    return new long[]{timestamps.get(0).timestamp.getTime(), timestamps.get(timestamps.size() - 1).timestamp.getTime()};

                currentEvent = seq.get(i);
            }
        }

        return new long[]{-1, -1};
    }

    public Set<Event> getUniqueEvents() {
        return new HashSet<Event>(seq);
    }

    /**
     * Get the second to last event of the sequence concatenated with a delimiter (Currently not used)
     *
     * @param delim The delimiter to put in front of the second to last event
     * @return the second to last event of the sequence concatenated with a delimiter
     */
    public String getSecondLastEventDelimited(String delim) {
        if (seq.isEmpty() || seq.size() == 1)
            return "";

        return seq.get(seq.size() - 2) + delim;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 97 * hash + Objects.hashCode(this.seq);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Sequence other = (Sequence) obj;
        if (!Objects.equals(this.seq, other.seq)) {
            return false;
        }
        return true;
    }
}
