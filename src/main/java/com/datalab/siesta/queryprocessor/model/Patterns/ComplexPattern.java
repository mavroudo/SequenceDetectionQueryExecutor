package com.datalab.siesta.queryprocessor.model.Patterns;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Events.EventSymbol;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


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
    public Set<EventPair> extractPairsWithSymbols() {
        return this.splitIntoSimples().stream()
                .flatMap(x ->
                        this.extractPairsAll(x,this.constraints).stream())
                .collect(Collectors.toSet());
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

    public List<String> getEventTypes(){
        return this.eventsWithSymbols.stream().map(Event::getName).collect(Collectors.toList());
    }
}
