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
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

public class EvaluateNewQueries {

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
        State[] states = new State[3];
        states[0] = new State( 1, "a", "A", "normal");
        List<String> s = new ArrayList<>();
        s.add("C");
        s.add("D");
        states[1] = new AdditionalState( 2, "a", s, "or");
        states[2] = new State( 3, "a", "B", "normal");
        nfaWrapper.setStates(states);
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
        State[] states = new State[3];
        states[0] = new State( 1, "a", "A", "normal");
        states[1] = new AdditionalState( 2, "a", "C", "negative");
        states[2] = new State( 3, "a", "B", "normal");
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(3);
        nfaWrapper.setStates(states);
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
        State[] states = new State[3];
        states[0] = new State( 1, "a", "A", "normal");
        states[1] = new AdditionalState( 2, "a", "B", "kleeneClosure*");
        states[2] = new State( 3, "a", "E", "normal");
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(3);
        nfaWrapper.setStates(states);
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
        State[] states = new State[2];
        states[0] = new AdditionalState( 1, "a", "A", "kleeneClosure*");
        states[1] = new State( 2, "a", "B", "normal");
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(2);
        nfaWrapper.setStates(states);
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
        State[] states = new State[3];
        states[0] = new AdditionalState( 1, "a", "A", "kleeneClosure*");
        states[1] = new State( 2, "a", "B", "normal");
        states[2] = new State( 3, "a", "E", "normal");
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(3);
        nfaWrapper.setStates(states);
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
        State[] states = new State[2];
        states[1] = new AdditionalState( 2, "a", "B", "kleeneClosure*");
        states[0] = new State( 1, "a", "A", "normal");
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(2);
        nfaWrapper.setStates(states);
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
        State[] states = new State[3];
        states[0] = new State( 1, "a", "A", "normal");
        states[1] = new State( 2, "a", "B", "normal");
        states[2] = new AdditionalState( 3, "a", "A", "kleeneClosure*");

        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(3);
        nfaWrapper.setStates(states);
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
        State[] states = new State[3];
        states[0] = new AdditionalState( 1, "a", "A", "negative");
        states[1] = new AdditionalState( 2, "a", "B", "normal");
        states[2] = new State( 3, "a", "C", "normal");
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(3);
        nfaWrapper.setStates(states);
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
        State[] states = new State[3];
        states[0] = new State( 1, "a", "B", "normal");
        states[1] = new AdditionalState( 2, "a", "C", "normal");
        states[2] = new AdditionalState( 3, "a", "A", "negative");
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(3);
        nfaWrapper.setStates(states);
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
        State[] states = new State[3];
        states[0] = new State( 1, "a", "A", "normal");
        states[1] = new AdditionalState( 2, "a", "B", "normal");
        states[2] = new AdditionalState( 3, "a", "C", "normal");
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(3);
        nfaWrapper.setStates(states);
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
    void testNormalEmpty(){
        State[] states = new State[3];
        states[0] = new State( 1, "a", "A", "normal");
        states[1] = new AdditionalState( 2, "a", "E", "normal");
        states[2] = new AdditionalState( 3, "a", "C", "normal");
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(3);
        nfaWrapper.setStates(states);
        ec.setNfa(new NFA(nfaWrapper));
        ec.initializeEngine();
        ec.setInput(this.getStream());
        try {
            ec.runEngine();
        } catch (CloneNotSupportedException | EvaluationException e) {
            throw new RuntimeException(e);
        }

        List<Match> matches = ec.getMatches();
        Assertions.assertEquals(0,matches.size());
    }

    @Test
    void testNormal2States(){
        State[] states = new State[2];
        states[0] = new State( 1, "a", "A", "normal");
        states[1] = new AdditionalState( 2, "a", "B", "normal");
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(2);
        nfaWrapper.setStates(states);
        ec.setNfa(new NFA(nfaWrapper));
        ec.initializeEngine();
        ec.setInput(this.getStream());
        try {
            ec.runEngine();
        } catch (CloneNotSupportedException | EvaluationException e) {
            throw new RuntimeException(e);
        }

        List<Match> matches = ec.getMatches();
        Assertions.assertEquals(3,matches.size());
    }

    @Test
    void testOrBeginState() {
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(2);
        State[] states = new State[2];
        List<String> s = new ArrayList<>();
        s.add("A");
        s.add("B");
        states[0] = new AdditionalState( 1, "a", s, "or");
        states[1] = new State( 2, "a", "C", "normal");
        nfaWrapper.setStates(states);
        ec.setNfa(new NFA(nfaWrapper));
        ec.initializeEngine();
        ec.setInput(this.getStream());
        try {
            ec.runEngine();
        } catch (CloneNotSupportedException | EvaluationException e) {
            throw new RuntimeException(e);
        }
        List<Match> matches = ec.getMatches();
        Assertions.assertEquals(3,matches.size());
    }

    @Test
    void testOrMiddleState() {
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(3);
        State[] states = new State[3];
        states[0] = new State( 1, "a", "D", "normal");
        List<String> s = new ArrayList<>();
        s.add("A");
        s.add("B");
        states[1] = new AdditionalState( 2, "a", s, "or");
        states[2] = new State(3 , "a", "E", "normal");
        nfaWrapper.setStates(states);
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
    void testKleenePlusBeginState() {
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(3);
        State[] states = new State[3];
        states[0] = new State( 1, "a", "A", "kleeneClosure");
        states[1] = new AdditionalState( 2, "a", "B", "normal");
        states[2] = new State(3 , "a", "E", "normal");
        nfaWrapper.setStates(states);
        ec.setNfa(new NFA(nfaWrapper));
        ec.initializeEngine();
        ec.setInput(this.getStream());
        try {
            ec.runEngine();
        } catch (CloneNotSupportedException | EvaluationException e) {
            throw new RuntimeException(e);
        }
        List<Match> matches = ec.getMatches();
        Assertions.assertEquals(6,matches.size());
    }

    @Test
    void testKleenePlusMiddleState() {
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(3);
        State[] states = new State[3];
        states[0] = new AdditionalState( 1, "a", "A", "normal");
        states[1] = new AdditionalState( 2, "a", "B", "kleeneClosure");
        states[2] = new State(3 , "a", "E", "normal");
        nfaWrapper.setStates(states);
        ec.setNfa(new NFA(nfaWrapper));
        ec.initializeEngine();
        ec.setInput(this.getStream());
        try {
            ec.runEngine();
        } catch (CloneNotSupportedException | EvaluationException e) {
            throw new RuntimeException(e);
        }
        List<Match> matches = ec.getMatches();
        Assertions.assertEquals(4,matches.size());
    }

    @Test
    void testKleenePlusEndState() {
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(3);
        State[] states = new State[3];
        states[0] = new AdditionalState( 1, "a", "A", "normal");
        states[1] = new AdditionalState( 2, "a", "B", "normal");
        states[2] = new AdditionalState(3 , "a", "A", "kleeneClosure");
        nfaWrapper.setStates(states);
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
}
