package com.datalab.siesta.queryprocessor.model.Patterns;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraint;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraint;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.ExtractedPairsForPatternDetection;
import edu.umass.cs.sase.query.State;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class SIESTAPattern {

//    protected Set<EventPair> extractPairsAll(List<EventPos> events, List<Constraint> constraints){
//        Set<EventPair> eventPairs = new HashSet<>();
//        for (int i = 0; i < events.size() - 1; i++) {
//            for (int j = i+1; j < events.size(); j++) {
//                EventPair n = new EventPair(events.get(i), events.get(j));
//                Constraint c = this.searchForConstraint(i, j,constraints);
//                if (c != null) n.setConstraint(c);
//                eventPairs.add(n);
//            }
//        }
//        return eventPairs;
//    }

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
            if (positionOfConstraints.contains(i))
                pairs.addPair(new EventPair(events.get(i), events.get(i))); //get double pairs
            for (int j = i + 1; j < events.size(); j++) { //get the rest of the pairs
                pairs.addTruePair(new EventPair(events.get(i), events.get(j)));
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

    protected Constraint searchForConstraint(int posA, int posB, List<Constraint> constraints) {
        for (Constraint c : constraints) {
            if (c.getPosA() == posA && c.getPosB() == posB) {
                return c;
            }
        }
        return null;
    }

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
     * @param i equal to the number of state -1
     * @return a list of the preicates for this state
     */
    protected List<String> generatePredicates(int i, List<Constraint> constraints){
        List<String> response = new ArrayList<>();
        for(Constraint c: constraints){
            if(c.getPosB()==i && c instanceof GapConstraint){
                GapConstraint gc = (GapConstraint) c;
                if(gc.getMethod().equals("within")) response.add(String.format(" position <= $%d.position + %d ",c.getPosA()+1,gc.getConstraint()));
                else response.add(String.format(" position >= $%d.position + %d ",c.getPosA()+1,gc.getConstraint())); //atleast
            }else if(c.getPosB()==i && c instanceof TimeConstraint){
                TimeConstraint tc = (TimeConstraint) c;
                if(tc.getMethod().equals("within")) response.add(String.format(" timestamp <= $%d.timestamp + %d ",c.getPosA()+1,tc.getConstraint()));
                else response.add(String.format(" timestamp >= $%d.timestamp + %d ",c.getPosA()+1,tc.getConstraint())); //atleast
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
