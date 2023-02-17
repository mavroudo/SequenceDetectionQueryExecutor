package com.datalab.siesta.queryprocessor.model;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import org.codehaus.jackson.annotate.JsonIgnore;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class PossibleOrderOfEvents {

    private long trace_id;
    private List<Event> events;
    private int posEventChanged;
    private long change;
    private boolean hasChanged;
    private List<Constraint> constraints;
    private int k;

    /**
     * Constructor of the original PossiblePatterns (before changes)
     *
     * @param trace_id
     * @param events
     * @param constraints
     */
    public PossibleOrderOfEvents(long trace_id, List<Event> events, List<Constraint> constraints, int k) {
        this.hasChanged = false;
        this.change = 0;
        this.posEventChanged = -1;
        this.trace_id = trace_id;
        this.events = events;
        this.constraints = constraints;
        this.k = k;
    }

    /**
     * Constructor of the possible modifications that can take place
     *
     * @param trace_id
     * @param events
     * @param posEventChanged
     * @param change
     * @param constraints
     * @param k
     */
    public PossibleOrderOfEvents(long trace_id, List<Event> events, int posEventChanged, long change,
                                 List<Constraint> constraints, int k, boolean hasChanged) {
        this.hasChanged = hasChanged;
        this.trace_id = trace_id;
        this.events = events;
        this.posEventChanged = posEventChanged;
        this.change = change;
        this.constraints = constraints;
        this.k = k;
    }

    @JsonIgnore
    public List<PossibleOrderOfEvents> getPossibleOrderOfEvents() {
        List<PossibleOrderOfEvents> order = new ArrayList<>();
        order.add(this);
        List<Tuple2<Integer, String>> constraintTypes = new ArrayList<>();
        this.constraints.forEach(constraint -> {
                if (constraint.getMethod().equals("within")) {
                    constraintTypes.add(new Tuple2<>(constraint.getPosA(), "front")); //allowed movement
                    constraintTypes.add(new Tuple2<>(constraint.getPosB(), "back"));
                } else {
                    constraintTypes.add(new Tuple2<>(constraint.getPosA(), "back"));
                    constraintTypes.add(new Tuple2<>(constraint.getPosB(), "front"));
                }
        });

        for (Tuple2<Integer, String> c : constraintTypes) {
            boolean stop = false;
            if (c._2.equals("front")) {
                int e = c._1 + 1; //starting from the next event
                while (!stop && e < this.events.size()) {
                    long diff = this.events.get(c._1).calculateDiff(this.events.get(e));
                    if (diff < k) {
                        List<Event> newEvents = new ArrayList<>() {
                            {
                                for (Event event : events) {
                                    this.add(event.clone());
                                }
                            }
                        };
                        long prevValue = newEvents.get(c._1).getPrimaryMetric();
                        newEvents.get(c._1).setPrimaryMetric(prevValue + diff + 1);
                        Collections.swap(newEvents, c._1, e);
                        order.add(new PossibleOrderOfEvents(trace_id, newEvents, e, diff + 1, constraints, k, true));
                        e += 1;
                    } else {
                        stop = true;
                    }
                }
            } else {
                int e = c._1 - 1; //starting from the previous event
                while (!stop && e >= 0) {
                    long diff = this.events.get(c._1).calculateDiff(this.events.get(e));
                    if (Math.abs(diff) < k) {
                        List<Event> newEvents = new ArrayList<>() {
                            {
                                for (Event event : events) {
                                    this.add(event.clone());
                                }
                            }
                        };
                        long prevValue = newEvents.get(c._1).getPrimaryMetric();
                        newEvents.get(c._1).setPrimaryMetric(prevValue + diff - 1);
                        Collections.swap(newEvents, c._1, e);
                        order.add(new PossibleOrderOfEvents(trace_id, newEvents, e, diff - 1, constraints, k, true));
                        e -= 1;
                    } else {
                        stop = true;
                    }
                }
            }
        }
        return order;
    }

    @JsonIgnore
    public boolean evaluatePatternConstraints(Occurrence occurrence) {
        //TODO: implement this logic
        //return true if the events in the occurrence meet the constraints by changing one particular event up to k
        List<Constraint> didntPass = this.passConstraints(occurrence.getOccurrence());
        if (didntPass.isEmpty()) return true;
        else {
            //maybe consider evaluating all
            List<ChangeRequired> changes = evaluateRemainingConstraints(didntPass, occurrence.getOccurrence());
            if (changes == null) return false;

        }

        return false;
    }

    private class ChangeRequired {
        protected int pos;
        protected int cons_id;
        protected long minChange;
        protected long maxChange;

        public ChangeRequired(int pos, int cons_id, long minChange, long maxChange) {
            this.pos = pos;
            this.cons_id = cons_id;
            this.minChange = minChange;
            this.maxChange = maxChange;
        }

        public int getPos() {
            return pos;
        }

        public int getCons_id() {
            return cons_id;
        }

        public long getMinChange() {
            return minChange;
        }

        public long getMaxChange() {
            return maxChange;
        }
    }

    private boolean solveChanges(List<ChangeRequired> changes) {
        //check one by one the different changes and once one position is found that can solve all constraints
        // (which means evaluate on the total amount of constraints) -> use
        return false;
    }

    /**
     * Method will return null if the constraints cannot be met even if changing further the values of the events
     *
     * @param constraints
     * @param eb
     * @return
     */
    private List<ChangeRequired> evaluateRemainingConstraints(List<Constraint> constraints, List<EventBoth> eb) {
        List<ChangeRequired> cr = new ArrayList<>();
        for (int i = 0; i < constraints.size(); i++) {
            Constraint c = constraints.get(i);
            long minChange = c.minimumChangeRequired(eb.get(c.getPosA()), eb.get(c.getPosB()));
            //TODO: ensure out of bounds exception for diffA and diffB

            long diffA = c.getMethod().equals("within") ? this.events.get(c.getPosA())
                    .calculateDiff(this.events.get(c.getPosA() + 1))
                    : this.events.get(c.getPosA()).calculateDiff(this.events.get(c.getPosA() - 1));
            long diffB = c.getMethod().equals("within") ? this.events.get(c.getPosB())
                    .calculateDiff(this.events.get(c.getPosB() - 1))
                    : this.events.get(c.getPosB()).calculateDiff(this.events.get(c.getPosB() + 1));
            if (this.hasChanged) { //only interested in this position
                if (c.getPosA() == this.posEventChanged) {
                    //the difference between the next or previous event, in order to not change order
                    long maxChange = Math.min(Math.abs(diffA) - 1, k - Math.abs(change)); //-1 so not overlap
                    if (minChange > maxChange) return null; //cannot be fulfilled
                    if (c.getMethod().equals("within"))
                        cr.add(new ChangeRequired(c.getPosA(), i, minChange, maxChange));
                    else cr.add(new ChangeRequired(c.getPosA(), i, -minChange, -maxChange));
                } else if (c.getPosB() == this.posEventChanged) {
                    //the difference between the next or previous event, in order to not change order
                    long maxChange = Math.min(Math.abs(diffB) - 1, k - Math.abs(change)); //-1 so not overlap
                    if (minChange > maxChange) return null; //cannot be fulfilled
                    if (c.getMethod().equals("within"))
                        cr.add(new ChangeRequired(c.getPosB(), i, -minChange, -maxChange));
                    else cr.add(new ChangeRequired(c.getPosB(), i, minChange, maxChange));
                } else {
                    return null;
                }
            } else { //this is an original pattern => any position can fulfill the requirement
                //do the same for the First Event
                long maxChange = Math.min(Math.abs(diffA) - 1, k - Math.abs(change)); //-1 so not overlap
                if (minChange > maxChange) return null; //cannot be fulfilled
                if (c.getMethod().equals("within")) cr.add(new ChangeRequired(c.getPosA(), i, minChange, maxChange));
                else cr.add(new ChangeRequired(c.getPosA(), i, -minChange, -maxChange));

                //do the same for the Second Event
                maxChange = Math.min(Math.abs(diffB) - 1, k - Math.abs(change)); //-1 so not overlap
                if (minChange > maxChange) return null; //cannot be fulfilled
                if (c.getMethod().equals("within")) cr.add(new ChangeRequired(c.getPosB(), i, -minChange, -maxChange));
                else cr.add(new ChangeRequired(c.getPosB(), i, minChange, maxChange));
            }
        }
        if (!this.hasChanged) {
            List<ChangeRequired> c = cr.stream()
                    .collect(Collectors.groupingBy(ChangeRequired::getPos))
                    .entrySet().stream().filter(x -> x.getValue().size() == constraints.size())
                    .flatMap(x -> x.getValue().stream()).collect(Collectors.toList());
            if (c.isEmpty())
                return null; //if there is no one position that changing it can solve all constraints we return null
            return c;
        } else {
            return cr;
        }
    }


    private List<Constraint> passConstraints(List<EventBoth> eb) {
        List<Constraint> didntPass = new ArrayList<>();
        for (Constraint c : this.constraints) {
            if (!c.isCorrect(eb.get(c.getPosA()), eb.get(c.getPosB()))) didntPass.add(c);
        }
        return didntPass;
    }

    public long getTrace_id() {
        return trace_id;
    }

    public void setTrace_id(long trace_id) {
        this.trace_id = trace_id;
    }

    public List<Event> getEvents() {
        return events;
    }

    public void setEvents(List<Event> events) {
        this.events = events;
    }

    public int getPosEventChanged() {
        return posEventChanged;
    }

    public void setPosEventChanged(int posEventChanged) {
        this.posEventChanged = posEventChanged;
    }

    public long getChange() {
        return change;
    }

    public void setChange(long change) {
        this.change = change;
    }

    public boolean isHasChanged() {
        return hasChanged;
    }

    public void setHasChanged(boolean hasChanged) {
        this.hasChanged = hasChanged;
    }

    public List<Constraint> getConstraints() {
        return constraints;
    }

    public void setConstraints(List<Constraint> constraints) {
        this.constraints = constraints;
    }

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PossibleOrderOfEvents that = (PossibleOrderOfEvents) o;
        return trace_id == that.trace_id && events.equals(that.events);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trace_id, events);
    }
}
