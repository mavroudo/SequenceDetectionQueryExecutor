package com.datalab.siesta.queryprocessor.model.Patterns;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraint;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraint;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.ExtractedPairsForPatternDetection;
import edu.umass.cs.sase.query.State;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Abstract class of the patterns that are defined in SIESTA. There are 2 types: (1) the simple ones and the (2)
 * complex ones that contains more sophisticated operators like the Kleene, the and, or etc.
 */
public abstract class SIESTAPattern {


    /**
     * Extract all the pairs that should are contained in the pattern
     *
     * @param events      the events we to be detected
     * @param constraints the constraints between two events
     * @return all the event pairs with the constraints embedded in
     */
    protected ExtractedPairsForPatternDetection extractPairsForPatternDetection(List<EventPos> events, List<Constraint> constraints, boolean fromOrTillSet) {
        ExtractedPairsForPatternDetection pairs = new ExtractedPairsForPatternDetection();
        Set<Integer> positionOfConstraints = constraints.stream().flatMap((Function<Constraint, Stream<Integer>>) x -> {
            List<Integer> l = new ArrayList<>();
            l.add(x.getPosA());
            l.add(x.getPosB());
            return l.stream();
        }).collect(Collectors.toSet());

//        Set<EventPair> trueEventPairs = new HashSet<>();
        for (int i = 0; i < events.size() - 1; i++) {
            if (positionOfConstraints.contains(events.get(i).getPosition()))
                pairs.addPair(new EventPair(events.get(i), events.get(i))); //get double pairs
            for (int j = i + 1; j < events.size(); j++) { //get the rest of the pairs
                // add constraints if exist
                boolean added = false;
                for (Constraint c : constraints) {
                    if (c.getPosA() == events.get(i).getPosition() && c.getPosB() == events.get(j).getPosition()) {
                        pairs.addTruePair(new EventPair(events.get(i), events.get(j), c));
                        added = true;
                    }
                }
                if (!added) {
                    pairs.addTruePair(new EventPair(events.get(i), events.get(j)));
                }
            }
        }


        if (fromOrTillSet) {
            for (EventPos e : events) {
                pairs.addPair(new EventPair(e, e));
            }
        }
        pairs.addPairs(pairs.getTruePairs());
        return pairs;
    }

    /**
     * Returns if there is a pattern constraint between two positions in the pattern
     *
     * @param posA        first position
     * @param posB        second position
     * @param constraints a list of constraints
     * @return the constraint between posA and posB if exists, or null otherwise
     */
    protected Constraint searchForConstraint(int posA, int posB, List<Constraint> constraints) {
        for (Constraint c : constraints) {
            if (c.getPosA() == posA && c.getPosB() == posB) {
                return c;
            }
        }
        return null;
    }

    /**
     * Extracts the consecutive event pairs along with their constraints. For example for the pattern A-B-C, with
     * constraint between 0 and 1, it will create two EventPairs: A,B (with the constraint) and B,C
     *
     * @param events      list of events in the query pattern
     * @param constraints list of constraints in the pattern query
     * @return the consecutive event pairs
     */
    protected Set<EventPair> extractPairsConsecutive(List<EventPos> events, List<Constraint> constraints) {
        Set<EventPair> eventPairs = new HashSet<>();
        for (int i = 0; i < events.size() - 1; i++) {
            EventPair n = new EventPair(events.get(i), events.get(i + 1));
            Constraint c = this.searchForConstraint(i, i + 1, constraints);
            if (c != null) n.setConstraint(c);
            eventPairs.add(n);
        }
        return eventPairs;
    }

//    protected List<Constraint> fixConstraints(List<Constraint> constraints) {
//        List<Constraint> newConstraints = new ArrayList<>();
//        for (Constraint c : constraints) {
//            if (c.getPosB() > c.getPosA() + 1) { //multiple gaps
//                for (int i = c.getPosA(); i < c.getPosB(); i++) {
//                    Constraint c1 = c.clone();
//                    c1.setPosA(i);
//                    c1.setPosB(i + 1);
//                    newConstraints.add(c1);
//                }
//            } else if (c.getPosB() == c.getPosA() + 1) {
//                newConstraints.add(c);
//            }
//        }
//        return newConstraints;
//    }

    /**
     * Generate the strings that dictates the predicates. i value is equal to state number -1, and that is because
     * states starts from 1 and states[] startrs from 0
     *
     * @param i equal to the number of state -1
     * @return a list of the preicates for this state
     */
    protected List<String> generatePredicates(int i, List<Constraint> constraints) {
        List<String> response = new ArrayList<>();
        for (Constraint c : constraints) {
            if (c.getPosB() == i && c instanceof GapConstraint) {
                GapConstraint gc = (GapConstraint) c;
                if (gc.getMethod().equals("within"))
                    response.add(String.format(" position <= $%d.position + %d ", c.getPosA() + 1, gc.getConstraint()));
                else
                    response.add(String.format(" position >= $%d.position + %d ", c.getPosA() + 1, gc.getConstraint())); //atleast
            } else if (c.getPosB() == i && c instanceof TimeConstraint) {
                TimeConstraint tc = (TimeConstraint) c;
                if (tc.getMethod().equals("within"))
                    response.add(String.format(" timestamp <= $%d.timestamp + %d ", c.getPosA() + 1, tc.getConstraintInSeconds()));
                else
                    response.add(String.format(" timestamp >= $%d.timestamp + %d ", c.getPosA() + 1, tc.getConstraintInSeconds())); //atleast
            }
        }
        return response;
    }

    public State[] getNfa() {
        return new State[1];
    }

    public State[] getNfaWithoutConstraints() {
        return new State[1];
    }

    public int getSize() {
        return 0;
    }

    public Set<String> getEventTypes() {
        return new HashSet<>();
    }
}
