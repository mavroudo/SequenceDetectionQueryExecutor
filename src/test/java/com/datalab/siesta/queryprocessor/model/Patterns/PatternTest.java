package com.datalab.siesta.queryprocessor.model.Patterns;

import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventSymbol;
import com.datalab.siesta.queryprocessor.model.ExtractedPairsForPatternDetection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.util.Assert;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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

        List<ExtractedPairsForPatternDetection> pairs = p.extractPairsForPatternDetection(false);
        Assertions.assertEquals(1,pairs.size());
        Assertions.assertEquals(3,pairs.get(0).getAllPairs().size());
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
        List<ExtractedPairsForPatternDetection> pairs = p.extractPairsForPatternDetection(false);
        //returns two patterns
        Assertions.assertEquals(2,pairs.size());
        Assertions.assertEquals(3,pairs.get(0).getTruePairs().size());
        Assertions.assertEquals(3,pairs.get(1).getTruePairs().size());
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
        List<ExtractedPairsForPatternDetection> pairs = p.extractPairsForPatternDetection(false);
        Assertions.assertEquals(1,pairs.size());
        Assertions.assertEquals(3,pairs.get(0).getTruePairs().size());
        Assertions.assertEquals(3,pairs.get(0).getTruePairs().size());
    }
}