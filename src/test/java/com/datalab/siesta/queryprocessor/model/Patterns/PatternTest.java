package com.datalab.siesta.queryprocessor.model.Patterns;

import com.datalab.siesta.queryprocessor.model.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Events.EventSymbol;
import org.junit.jupiter.api.Test;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class PatternTest {

    @Test
    void extractPairsSimple() {
        EventSymbol es1 = new EventSymbol("A",0,"");
        EventSymbol es2 = new EventSymbol("B",1,"");
        EventSymbol es3 = new EventSymbol("C",2,"");
        List<EventSymbol> events = new ArrayList<>();
        events.add(es1);
        events.add(es2);
        events.add(es3);
        ComplexPattern p = new ComplexPattern(events);

        Set<EventPair> pairs = p.extractPairsWithSymbols();
        Assert.isTrue(pairs.size()==3);
    }
    @Test
    void extractPairsOr(){
        EventSymbol es1 = new EventSymbol("A",0,"");
        EventSymbol es2 = new EventSymbol("B",1,"");
        EventSymbol es3 = new EventSymbol("C",1,"");
        EventSymbol es4 = new EventSymbol("D",2,"");
        List<EventSymbol> events = new ArrayList<>();
        events.add(es1);
        events.add(es2);
        events.add(es3);
        events.add(es4);
        ComplexPattern p = new ComplexPattern();
        p.setEventsWithSymbols(events);
        Set<EventPair> pairs = p.extractPairsWithSymbols();
        Assert.isTrue(pairs.size()==5);
    }

    @Test
    void extractPairsNotOr(){
        EventSymbol es1 = new EventSymbol("A",0,"");
        EventSymbol es2 = new EventSymbol("B",1,"not");
        EventSymbol es3 = new EventSymbol("C",1,"");
        EventSymbol es4 = new EventSymbol("D",2,"");
        List<EventSymbol> events = new ArrayList<>();
        events.add(es1);
        events.add(es2);
        events.add(es3);
        events.add(es4);
        ComplexPattern p = new ComplexPattern();
        p.setEventsWithSymbols(events);
        Set<EventPair> pairs = p.extractPairsWithSymbols();
        Assert.isTrue(pairs.size()==3);
    }

    @Test
    void extractPairsKleene(){
        EventSymbol es1 = new EventSymbol("A",0,"");
        EventSymbol es2 = new EventSymbol("B",1,"+");
        EventSymbol es3 = new EventSymbol("C",2,"");
        EventSymbol es4 = new EventSymbol("D",3,"*");
        List<EventSymbol> events = new ArrayList<>();
        events.add(es1);
        events.add(es2);
        events.add(es3);
        events.add(es4);
        ComplexPattern p = new ComplexPattern();
        p.setEventsWithSymbols(events);
        Set<EventPair> pairs = p.extractPairsWithSymbols();
        Assert.isTrue(pairs.size()==3);
    }
}