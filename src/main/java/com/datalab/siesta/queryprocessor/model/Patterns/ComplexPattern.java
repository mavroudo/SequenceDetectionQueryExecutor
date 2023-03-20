package com.datalab.siesta.queryprocessor.model.Patterns;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Events.EventSymbol;
import com.datalab.siesta.queryprocessor.model.ExtractedPairsForPatternDetection;
import edu.umass.cs.sase.query.State;
import org.codehaus.jackson.annotate.JsonIgnore;
import scala.Tuple2;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class ComplexPattern extends SIESTAPattern{

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

    public List<Constraint> getConsecutiveConstraints(){ return this.fixConstraints(this.constraints);}

    public ComplexPattern(List<EventSymbol> eventsWithSymbols, List<Constraint> constraints) {
        this.eventsWithSymbols = eventsWithSymbols;
        this.constraints = constraints;
    }

    public ComplexPattern(List<EventSymbol> eventsWithSymbols) {
        this.eventsWithSymbols = eventsWithSymbols;
        this.constraints=new ArrayList<>();
    }

    public ComplexPattern() {
        this.constraints=new ArrayList<>();
        this.eventsWithSymbols=new ArrayList<>();
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


//    @JsonIgnore
//    public ExtractedPairsForPatternDetection extractPairsForPatternDetection(boolean fromOrTillSet){
//        ExtractedPairsForPatternDetection pairs = new ExtractedPairsForPatternDetection();
//        for(List<EventPos> events : this.splitIntoSimples()){
//            ExtractedPairsForPatternDetection s = this.extractPairsForPatternDetection(events,this.getConstraints(),fromOrTillSet);
//            pairs.addPairs(s.getAllPairs());
//            pairs.addTruePairs(s.getTruePairs());
//        }
//        return pairs;
//    }

    @JsonIgnore
    public ExtractedPairsForPatternDetection extractPairsForPatternDetection(boolean fromOrTillSet){
        List<ExtractedPairsForPatternDetection> elist = new ArrayList<>();
        for(List<EventSymbol> events:this.splitWithOr()){
            List<EventPos> l = new ArrayList<>();
            List<EventPair> allPairs = new ArrayList<>();
            for(int i=0;i<events.size();i++){
                EventSymbol es = events.get(i);
                switch (es.getSymbol()) {
                    case "_":
                    case "":
                        l.add(new EventPos(es.getName(), i));
                        break;
                    case "+":
                        l.add(new EventPos(es.getName(), i));
                        allPairs.add(new EventPair(new Event(es.getName()), new Event(es.getName())));
                        break;
                    case "*":
                    case "!":
                        allPairs.add(new EventPair(new Event(es.getName()), new Event(es.getName())));
                        break;
                }
            }
            ExtractedPairsForPatternDetection s = this.extractPairsForPatternDetection(l,this.getConstraints(),fromOrTillSet);
            s.addPairs(allPairs);
            elist.add(s);
        }
        ExtractedPairsForPatternDetection all = elist.get(0);
        if(elist.size()>1){
            for(int i=1;i<elist.size();i++){
                all.addPairs(elist.get(i).getAllPairs());
                all.getTruePairs().retainAll(elist.get(i).getTruePairs());
            }
        }
        return all;
    }

    private Set<List<EventSymbol>> splitWithOr(){
        Set<List<EventSymbol>> l = new HashSet<>();
        l.add(new ArrayList<>());
        int last_pos = -1;
        for (EventSymbol e : eventsWithSymbols) {
            if (e.getPosition() == last_pos) { //That means it uses or
                for (List<EventSymbol> sl : l) {
                    List<EventSymbol> slnew = new ArrayList<>(sl);
                    try {
                        e.setSymbol("_");
                        slnew.set(last_pos, e);
                    } catch (IndexOutOfBoundsException exe) {
                        slnew.add(e);
                    }
                    l.add(slnew);
                }
            }else {
                last_pos = e.getPosition();
                for (List<EventSymbol> sl : l) {
                    sl.add(e);
                }
            }
        }
        return l;
    }

    private Set<List<EventPos>> splitIntoSimples() {
        Set<List<EventPos>> l = new HashSet<>();
        l.add(new ArrayList<>());
        int last_pos = -1;
        for (EventSymbol e : eventsWithSymbols) {
            if (e.getPosition() == last_pos) {
                for (List<EventPos> sl : l) {
                    List<EventPos> slnew = new ArrayList<>(sl);
                    try {
                        slnew.set(last_pos, e);
                    }catch (IndexOutOfBoundsException exe){
                        slnew.add(e);
                    }
                    l.add(slnew);
                }
            } else if (e.getSymbol().equals("+") || e.getSymbol().equals("")) {
                last_pos = e.getPosition();
                for (List<EventPos> sl : l) {
                    sl.add(e);
                }
            } else if (e.getSymbol().equals("not") || e.getSymbol().equals("*")) {
                last_pos = e.getPosition();
            }
        }
        return l;
    }

    @JsonIgnore
    @Override
    public Set<String> getEventTypes(){
        return this.eventsWithSymbols.stream().map(Event::getName).collect(Collectors.toSet());
    }

    @JsonIgnore
    public SimplePattern getItSimpler(){
        List<EventPos> response = new ArrayList<>();
        for(EventSymbol ep : this.eventsWithSymbols){
            if( ep.getSymbol().equals("")) response.add(ep);
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
        if (sp!=null) {
            return sp.getNfa();
        }else{
            return super.getNfa(); //TODO: implement it
        }
    }

    @JsonIgnore
    @Override
    public State[] getNfaWithoutConstraints() {
        SimplePattern sp = this.getItSimpler();
        if (sp!=null) {
            return sp.getNfaWithoutConstraints();
        }else{
            return super.getNfaWithoutConstraints(); //TODO: implement it
        }
    }

    @Override
    @JsonIgnore
    public int getSize() {
        return this.eventsWithSymbols.size();
    }
}
