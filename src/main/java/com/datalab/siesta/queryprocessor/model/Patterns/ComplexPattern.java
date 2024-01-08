package com.datalab.siesta.queryprocessor.model.Patterns;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Events.EventSymbol;
import com.datalab.siesta.queryprocessor.model.ExtractedPairsForPatternDetection;
import edu.umass.cs.sase.query.AdditionalState;
import edu.umass.cs.sase.query.State;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A subclass of the SIESTAPattern. Describes patterns that contain complex events, i.e. events with special operators
 * like Kleene*, Or and Not. The patterns described by this class is
 */
public class ComplexPattern extends SIESTAPattern {

    private List<EventSymbol> eventsWithSymbols;

    private List<Constraint> constraints;

    public List<EventSymbol> getEventsWithSymbols() {
        return eventsWithSymbols;
    }

    public void setEventsWithSymbols(List<EventSymbol> eventsWithSymbols) {
        this.eventsWithSymbols = eventsWithSymbols;
    }

    public List<Constraint> getConstraints() {
        return constraints;
    }

    public void setConstraints(List<Constraint> constraints) {
        this.constraints = constraints;
    }


    public ComplexPattern(List<EventSymbol> eventsWithSymbols, List<Constraint> constraints) {
        this.eventsWithSymbols = eventsWithSymbols;
        this.constraints = constraints;
    }

    public ComplexPattern(List<EventSymbol> eventsWithSymbols) {
        this.eventsWithSymbols = eventsWithSymbols;
        this.constraints = new ArrayList<>();
    }

    public ComplexPattern() {
        this.constraints = new ArrayList<>();
        this.eventsWithSymbols = new ArrayList<>();
    }

    /**
     * Extracting pairs based on the various symbols:
     * * -> this will not be considered for the time being
     * not -> this will also been removed
     * + -> this will be treated as a normal event
     * empty -> this is a normal event
     * In addition to the above we support or statement that can be identified with events
     * sharing the same position
     *
     * @return Pair of events along with the constraints
     */


    @JsonIgnore
    public List<ExtractedPairsForPatternDetection> extractPairsForPatternDetection(boolean fromOrTillSet) {
        List<ExtractedPairsForPatternDetection> elist = new ArrayList<>();
        Set<List<EventSymbol>> splitWithOr = this.splitWithOr();
        for (List<EventSymbol> events : splitWithOr) {
            List<EventPos> l = new ArrayList<>();
            List<EventPair> allPairs = new ArrayList<>();
            for (int i = 0; i < events.size(); i++) {
                EventSymbol es = events.get(i);
                switch (es.getSymbol()) {
                    case "_":
                    case "":
                        l.add(new EventPos(es.getName(), i));
                        break;
                    case "+":
                    case "!":
                        l.add(new EventPos(es.getName(), i));
                        allPairs.add(new EventPair(new Event(es.getName()), new Event(es.getName())));
                        break;
                    case "*":
                        allPairs.add(new EventPair(new Event(es.getName()), new Event(es.getName())));
                        break;
                }
            }
            ExtractedPairsForPatternDetection s = this.extractPairsForPatternDetection(l, this.getConstraints(), fromOrTillSet);
            s.addPairs(allPairs);
            elist.add(s);
        }
        return elist;
    }

    private Set<List<EventSymbol>> splitWithOr(){
        List<Set<EventSymbol>> samePos = new ArrayList<>();
        // split events to different sets based on their positions in the pattern
        for(EventSymbol e: this.eventsWithSymbols){
            EventSymbol es = new EventSymbol(e.getName(),e.getPosition(),e.getSymbol());
            if(e.getSymbol().equals("||")){
                es.setSymbol("_");
            }
            if(samePos.size()==e.getPosition()){
                Set<EventSymbol> temp = new HashSet<>();
                temp.add(es);
                samePos.add(temp);
            }else{
                samePos.get(e.getPosition()).add(es);
            }
        }
        List<List<EventSymbol>> result = new ArrayList<>();
        // recursively detect different patterns
        generateCombinations(samePos, 0, new ArrayList<>(), result);
        return new HashSet<>(result);

    }

    /**
     * Recursive function that finds the different patterns that are separated by or
     * @param eventSets a list of sets of events (each set corresponds to events that share the same position)
     * @param index the index od the set that it is currently explored
     * @param current list of combinations so far
     * @param result the list of all the different patterns
     */
    private void generateCombinations(List<Set<EventSymbol>> eventSets, int index, List<EventSymbol> current, List<List<EventSymbol>> result) {
        if (index == eventSets.size()) {
            result.add(new ArrayList<>(current));
            return;
        }

        Set<EventSymbol> currentSet = eventSets.get(index);
        for (EventSymbol event : currentSet) {
            current.add(event);
            generateCombinations(eventSets, index + 1, current, result);
            current.remove(current.size() - 1);
        }
    }

    private Set<List<EventSymbol>> splitWithOrOld() {
        Set<List<EventSymbol>> l = new HashSet<>();
        l.add(new ArrayList<>());
        int last_pos = -1;
        for (EventSymbol e : eventsWithSymbols) {
            if (e.getPosition() == last_pos) { //That means it uses or and this is the second, third, ... event
                for (List<EventSymbol> sl : l) {
                    List<EventSymbol> slnew = new ArrayList<>(sl);
                    try {
                        EventSymbol es = new EventSymbol(e.getName(),e.getPosition(),"_");
                        slnew.set(last_pos, es);
                    } catch (IndexOutOfBoundsException exe) {
                        slnew.add(e);
                    }
                    l.add(slnew);
                }
            } else {
                last_pos = e.getPosition();
                for (List<EventSymbol> sl : l) {
                    EventSymbol es = new EventSymbol(e.getName(),e.getPosition(),e.getSymbol());
                    if(e.getSymbol().equals("||")){
                        es.setSymbol("_");
                    }
                    sl.add(es);
                }
            }
        }
        return l;
    }

//    private Set<List<EventPos>> splitIntoSimples() {
//        Set<List<EventPos>> l = new HashSet<>();
//        l.add(new ArrayList<>());
//        int last_pos = -1;
//        for (EventSymbol e : eventsWithSymbols) {
//            if (e.getPosition() == last_pos) {
//                for (List<EventPos> sl : l) {
//                    List<EventPos> slnew = new ArrayList<>(sl);
//                    try {
//                        slnew.set(last_pos, e);
//                    }catch (IndexOutOfBoundsException exe){
//                        slnew.add(e);
//                    }
//                    l.add(slnew);
//                }
//            } else if (e.getSymbol().equals("+") || e.getSymbol().equals("")) {
//                last_pos = e.getPosition();
//                for (List<EventPos> sl : l) {
//                    sl.add(e);
//                }
//            } else if (e.getSymbol().equals("not") || e.getSymbol().equals("*")) {
//                last_pos = e.getPosition();
//            }
//        }
//        return l;
//    }

    @JsonIgnore
    @Override
    public Set<String> getEventTypes() {
        return this.eventsWithSymbols.stream().map(Event::getName).collect(Collectors.toSet());
    }

    @JsonIgnore
    public SimplePattern getItSimpler() {
        List<EventPos> response = new ArrayList<>();
        for (EventSymbol ep : this.eventsWithSymbols) {
            if (ep.getSymbol().isEmpty() ||ep.getSymbol().equals("_")) response.add(ep);
            else return null;
        }
        SimplePattern sp = new SimplePattern(response);
        sp.setConstraints(this.constraints);
        return sp;
    }

    @Override
    @JsonIgnore
    public State[] getNfa() {
        SimplePattern sp = this.getItSimpler();
        if (sp != null) {
            return sp.getNfa();
        } else {
            State[] states = this.getStatesWithoutConstraints();
            for(int m = 0;m< states.length;m++){ //adding constraints
                List<String> predicates = this.generatePredicates(m, this.constraints);
                State s = states[m];
                for(String predicate : predicates) {
                    if (s.getStateType().contains("kleeneClosure")) {
                        s.getEdges(1).addPredicate(predicate);
                    }
                    s.getEdges(0).addPredicate(predicate);
                }
//                s.getEdges(0).addPredicate();
//                this.generatePredicates(m, this.constraints).forEach(states[m]::addPredicate);
            }
            return states;
        }
    }

    private State[] getStatesWithoutConstraints(){
        State[] states = new State[this.eventsWithSymbols.size()];
        int i = 0;
        int order=1;
        while (i < this.eventsWithSymbols.size()) {
            EventSymbol es = this.eventsWithSymbols.get(i);
            switch (es.getSymbol()) {
                case "_":
                    State s = new State(order, "a", String.format("%s", es.getName()), "normal");
                    states[order-1] = s;
                    i++;
                    break;
                case "+":
                    State s2 = new State(order, "a", String.format("%s", es.getName()), "kleeneClosure");
                    states[order-1] = s2;
                    i++;
                    break;
                case "*":
                    State s3 = new AdditionalState(order, "a", String.format("%s", es.getName()), "kleeneClosure*");
                    states[order-1] = s3;
                    i++;
                    break;
                case "!":
                    State s4 = new AdditionalState(i + 1, "a", String.format("%s", es.getName()), "negative");
                    states[order-1] = s4;
                    i++;
                    break;
                case "||":
                    List<String> eventTypes = new ArrayList<>();
                    int k = i;
                    while (k < this.eventsWithSymbols.size()) {
                        if (this.eventsWithSymbols.get(k).getPosition() == es.getPosition()) {
                            eventTypes.add(this.eventsWithSymbols.get(k).getName());
                            k++;
                        }
                        else{
                            break;
                        }
                    }
                    State s5 = new AdditionalState(order, "a", eventTypes, "or");
                    states[order-1] = s5;
                    i = k;
                    break;
            }
            order++;

        }
        if(order<=this.eventsWithSymbols.size()) { //if no or exists then order will be equal to the size+1
            State[] newStates = new State[order - 1]; //in case of or this would be different
            for(i=0;i<order-1;i++){
                newStates[i]=states[i];
            }
            return newStates;
        }else {
            return states;
        }
    }

    @JsonIgnore
    @Override
    public State[] getNfaWithoutConstraints() {
        SimplePattern sp = this.getItSimpler();
        if (sp != null) {
            return sp.getNfaWithoutConstraints();
        } else {
            return this.getStatesWithoutConstraints();
        }
    }

    @Override
    @JsonIgnore
    public int getSize() {
        return this.eventsWithSymbols.size();
    }
}
