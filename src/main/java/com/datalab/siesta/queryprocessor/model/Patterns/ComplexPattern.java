package com.datalab.siesta.queryprocessor.model.Patterns;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Events.EventSymbol;
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


    @JsonIgnore
    public Tuple2<Integer,Set<EventPair> > extractPairsForPatternDetection(boolean fromOrTillSet){
        Set<EventPair> eventPairs = new HashSet<>();
        int n = Integer.MAX_VALUE;
        for(List<EventPos> events : this.splitIntoSimples()){
            Tuple2<Integer,Set<EventPair>> s = this.extractPairsForPatternDetection(events,this.getConstraints(),fromOrTillSet);
            eventPairs.addAll(s._2);
            n=Math.min(n,s._1);
        }

        return new Tuple2<>(n,eventPairs);
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
