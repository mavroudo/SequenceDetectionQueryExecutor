package com.datalab.siesta.queryprocessor.model.Patterns;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
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
     * @param events the events we to be detected
     * @param constraints the constraints between two events
     * @return all the event pairs with the constraints embedded in
     */
    protected Tuple2<Integer,Set<EventPair>> extractPairsForPatternDetection(List<EventPos> events, List<Constraint> constraints){
        Set<EventPair> allEventPairs = new HashSet<>();
        Set<Integer> positionOfConstraints = constraints.stream().flatMap((Function<Constraint, Stream<Integer>>)  x->{
            List<Integer> l = new ArrayList<>();
            l.add(x.getPosA());
            l.add(x.getPosB());
            return l.stream();
        }).collect(Collectors.toSet());
        Set<EventPair> trueEventPairs = new HashSet<>();
        for (int i = 0; i < events.size() - 1; i++) {
            if(positionOfConstraints.contains(i)) allEventPairs.add(new EventPair(events.get(i),events.get(i))); //get double pairs
            for (int j = i+1; j < events.size(); j++) { //get the rest of the pairs
                EventPair p = new EventPair(events.get(i), events.get(j));
                trueEventPairs.add(p);
            }
        }
        allEventPairs.addAll(trueEventPairs);
        return new Tuple2<>(trueEventPairs.size(), allEventPairs);
    }

    protected Constraint searchForConstraint(int posA, int posB, List<Constraint> constraints) {
        for (Constraint c : constraints) {
            if (c.getPosA() == posA && c.getPosB() == posB) {
                return c;
            }
        }
        return null;
    }

    protected Set<EventPair> extractPairsConsecutive(List<EventPos> events,List<Constraint> constraints){
        Set<EventPair> eventPairs = new HashSet<>();
        for(int i =0 ; i< events.size()-1; i++){
            EventPair n = new EventPair(events.get(i), events.get(i+1));
            Constraint c = this.searchForConstraint(i, i+1,constraints);
            if (c != null) n.setConstraint(c);
            eventPairs.add(n);
        }
        return eventPairs;
    }

    protected List<Constraint> fixConstraints(List<Constraint> constraints) {
        List<Constraint> newConstraints = new ArrayList<>();
        for (Constraint c : constraints) {
            if(c.getPosB()>c.getPosA()+1){ //multiple gaps
                for (int i = c.getPosA(); i < c.getPosB(); i++) {
                    Constraint c1 = c.clone();
                    c1.setPosA(i);
                    c1.setPosB(i+1);
                    newConstraints.add(c1);
                }
            }else if(c.getPosB()==c.getPosA()+1){
                newConstraints.add(c);
            }
        }
        return newConstraints;
    }

    public State[] getNfa(){
        return new State[1];
    }

    public State[] getNfaWithoutConstraints(){
        return new State[1];
    }

    public int getSize(){
        return 0;
    }

    public Set<String> getEventTypes(){
        return new HashSet<>();
    }
}
