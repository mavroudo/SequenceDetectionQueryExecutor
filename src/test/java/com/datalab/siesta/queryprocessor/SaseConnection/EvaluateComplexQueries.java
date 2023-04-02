package com.datalab.siesta.queryprocessor.SaseConnection;

import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Events.EventSymbol;
import com.datalab.siesta.queryprocessor.model.Patterns.ComplexPattern;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import edu.umass.cs.sase.engine.EngineController;
import edu.umass.cs.sase.engine.Match;
import edu.umass.cs.sase.query.AdditionalState;
import edu.umass.cs.sase.query.NFA;
import edu.umass.cs.sase.query.State;
import edu.umass.cs.sase.stream.Stream;
import net.sourceforge.jeval.EvaluationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class EvaluateComplexQueries {

    private Utils utils;
    public Stream getStream() {
        utils = new Utils();
        List<Event> el = new ArrayList<>();
        el.add(new EventPos("A", 0));
        el.add(new EventPos("B", 1));
        el.add(new EventPos("A", 2));
        el.add(new EventPos("C", 3));
        el.add(new EventPos("D", 4));
        el.add(new EventPos("A", 5));
        el.add(new EventPos("B", 6));
        el.add(new EventPos("E", 7));
        Stream s = new Stream(el.size());
        List<SaseEvent> saseEvents = utils.transformToSaseEvents(el);
        s.setEvents(saseEvents.toArray(new SaseEvent[el.size()]));
        return s;

    }

    @Test
    void testOrState() {
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(3);
        ComplexPattern cp = new ComplexPattern();
        List<EventSymbol> es = new ArrayList<>();
        es.add(new EventSymbol("A",0,"_"));
        es.add(new EventSymbol("C",1,"||"));
        es.add(new EventSymbol("D",1,"_"));
        es.add(new EventSymbol("B",2,"_"));
        cp.setEventsWithSymbols(es);
        nfaWrapper.setStates(cp.getNfa());
        ec.setNfa(new NFA(nfaWrapper));
        ec.initializeEngine();
        ec.setInput(this.getStream());
        try {
            ec.runEngine();
        } catch (CloneNotSupportedException | EvaluationException e) {
            throw new RuntimeException(e);
        }
        List<Match> matches = ec.getMatches();
        Assertions.assertEquals(2,matches.size());
    }

    @Test
    void testNegativeState(){
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(3);
        ComplexPattern cp = new ComplexPattern();
        List<EventSymbol> es = new ArrayList<>();
        es.add(new EventSymbol("A",0,"_"));
        es.add(new EventSymbol("C",1,"!"));
        es.add(new EventSymbol("B",2,"_"));
        cp.setEventsWithSymbols(es);
        nfaWrapper.setStates(cp.getNfa());
        ec.setNfa(new NFA(nfaWrapper));
        ec.initializeEngine();
        ec.setInput(this.getStream());
        try {
            ec.runEngine();
        } catch (CloneNotSupportedException | EvaluationException e) {
            throw new RuntimeException(e);
        }

        List<Match> matches = ec.getMatches();
        Assertions.assertEquals(2,matches.size());
    }

    @Test
    void testKleeneStarState(){
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(3);
        ComplexPattern cp = new ComplexPattern();
        List<EventSymbol> es = new ArrayList<>();
        es.add(new EventSymbol("A",0,"_"));
        es.add(new EventSymbol("B",1,"*"));
        es.add(new EventSymbol("E",1,"_"));
        cp.setEventsWithSymbols(es);
        nfaWrapper.setStates(cp.getNfa());
        ec.setNfa(new NFA(nfaWrapper));
        ec.initializeEngine();
        ec.setInput(this.getStream());
        try {
            ec.runEngine();
        } catch (CloneNotSupportedException | EvaluationException e) {
            throw new RuntimeException(e);
        }

        List<Match> matches = ec.getMatches();
        Assertions.assertEquals(7,matches.size());
    }

    @Test
    void testKleeneStarBeginState(){
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(2);
        ComplexPattern cp = new ComplexPattern();
        List<EventSymbol> es = new ArrayList<>();
        es.add(new EventSymbol("A",0,"*"));
        es.add(new EventSymbol("B",1,"_"));
        cp.setEventsWithSymbols(es);
        nfaWrapper.setStates(cp.getNfa());
        ec.setNfa(new NFA(nfaWrapper));
        ec.initializeEngine();
        ec.setInput(this.getStream());
        try {
            ec.runEngine();
        } catch (CloneNotSupportedException | EvaluationException e) {
            throw new RuntimeException(e);
        }

        List<Match> matches = ec.getMatches();
        Assertions.assertEquals(8,matches.size());
    }

    @Test
    void testKleeneStarBegin3State(){
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(3);
        ComplexPattern cp = new ComplexPattern();
        List<EventSymbol> es = new ArrayList<>();
        es.add(new EventSymbol("A",0,"*"));
        es.add(new EventSymbol("B",1,"_"));
        es.add(new EventSymbol("E",1,"_"));
        cp.setEventsWithSymbols(es);
        nfaWrapper.setStates(cp.getNfa());
        ec.setNfa(new NFA(nfaWrapper));
        ec.initializeEngine();
        ec.setInput(this.getStream());
        try {
            ec.runEngine();
        } catch (CloneNotSupportedException | EvaluationException e) {
            throw new RuntimeException(e);
        }

        List<Match> matches = ec.getMatches();
        Assertions.assertEquals(8,matches.size());
    }

    @Test
    void testKleeneStarEndState(){
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(2);
        ComplexPattern cp = new ComplexPattern();
        List<EventSymbol> es = new ArrayList<>();
        es.add(new EventSymbol("A",0,"_"));
        es.add(new EventSymbol("B",1,"*"));
        cp.setEventsWithSymbols(es);
        nfaWrapper.setStates(cp.getNfa());
        ec.setNfa(new NFA(nfaWrapper));
        ec.initializeEngine();
        ec.setInput(this.getStream());
        try {
            ec.runEngine();
        } catch (CloneNotSupportedException | EvaluationException e) {
            throw new RuntimeException(e);
        }

        List<Match> matches = ec.getMatches();
        Assertions.assertEquals(7,matches.size());
    }

    @Test
    void testKleeneStarEnd3State(){
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(3);
        ComplexPattern cp = new ComplexPattern();
        List<EventSymbol> es = new ArrayList<>();
        es.add(new EventSymbol("A",0,"_"));
        es.add(new EventSymbol("B",1,"_"));
        es.add(new EventSymbol("A",1,"*"));
        cp.setEventsWithSymbols(es);
        nfaWrapper.setStates(cp.getNfa());
        ec.setNfa(new NFA(nfaWrapper));
        ec.initializeEngine();
        ec.setInput(this.getStream());
        try {
            ec.runEngine();
        } catch (CloneNotSupportedException | EvaluationException e) {
            throw new RuntimeException(e);
        }

        List<Match> matches = ec.getMatches();
        Assertions.assertEquals(5,matches.size());
    }

    @Test
    void testNegativeBeginState(){
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(3);
        ComplexPattern cp = new ComplexPattern();
        List<EventSymbol> es = new ArrayList<>();
        es.add(new EventSymbol("A",0,"!"));
        es.add(new EventSymbol("B",1,"_"));
        es.add(new EventSymbol("C",1,"_"));
        cp.setEventsWithSymbols(es);
        nfaWrapper.setStates(cp.getNfa());
        ec.setNfa(new NFA(nfaWrapper));
        ec.initializeEngine();
        ec.setInput(this.getStream());
        try {
            ec.runEngine();
        } catch (CloneNotSupportedException | EvaluationException e) {
            throw new RuntimeException(e);
        }

        List<Match> matches = ec.getMatches();
        Assertions.assertEquals(1,matches.size());
    }

    @Test
    void testNegativeEndState(){
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(3);
        ComplexPattern cp = new ComplexPattern();
        List<EventSymbol> es = new ArrayList<>();
        es.add(new EventSymbol("B",0,"_"));
        es.add(new EventSymbol("C",1,"_"));
        es.add(new EventSymbol("A",1,"!"));
        cp.setEventsWithSymbols(es);
        nfaWrapper.setStates(cp.getNfa());
        ec.setNfa(new NFA(nfaWrapper));
        ec.initializeEngine();
        ec.setInput(this.getStream());
        try {
            ec.runEngine();
        } catch (CloneNotSupportedException | EvaluationException e) {
            throw new RuntimeException(e);
        }

        List<Match> matches = ec.getMatches();
        Assertions.assertEquals(1,matches.size());
    }



    @Test
    void testNormalStates(){
        ComplexPattern cp = new ComplexPattern();
        List<EventSymbol> es = new ArrayList<>();
        es.add(new EventSymbol("A",0,"_"));
        es.add(new EventSymbol("B",1,"_"));
        es.add(new EventSymbol("C",2,"_"));
        cp.setEventsWithSymbols(es);
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(3);
        nfaWrapper.setStates(cp.getNfa());
        ec.setNfa(new NFA(nfaWrapper));
        ec.initializeEngine();
        ec.setInput(this.getStream());
        try {
            ec.runEngine();
        } catch (CloneNotSupportedException | EvaluationException e) {
            throw new RuntimeException(e);
        }

        List<Match> matches = ec.getMatches();
        Assertions.assertEquals(1,matches.size());
    }

    @Test
    void multipleOrStar(){
        ComplexPattern cp = new ComplexPattern();
        List<EventSymbol> es = new ArrayList<>();
        es.add(new EventSymbol("A",0,"||"));
        es.add(new EventSymbol("B",0,"_"));
        es.add(new EventSymbol("B",1,"*"));
        es.add(new EventSymbol("E",2,"_"));
        cp.setEventsWithSymbols(es);
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(3);
        nfaWrapper.setStates(cp.getNfa());
        ec.setNfa(new NFA(nfaWrapper));
        ec.initializeEngine();
        ec.setInput(this.getStream());
        try {
            ec.runEngine();
        } catch (CloneNotSupportedException | EvaluationException e) {
            throw new RuntimeException(e);
        }

        List<Match> matches = ec.getMatches();
        Assertions.assertEquals(9,matches.size());
    }
}
