package com.datalab.siesta.queryprocessor.SaseConnection;

import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.WhyNotMatch.UsingSase.UncertainTimeEvent;
import edu.umass.cs.sase.engine.EngineController;
import edu.umass.cs.sase.engine.Match;
import edu.umass.cs.sase.query.NFA;
import edu.umass.cs.sase.query.State;
import edu.umass.cs.sase.stream.Stream;
import net.sourceforge.jeval.EvaluationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

//@SpringBootTest
class SaseConnectorTest {

//    @Autowired
//    private SaseConnector saseConnector;
//
//    @Autowired
//    private DBConnector dbConnector;
//
//    @Autowired
//    private QueryPatternDetection qpd;

//    @Test
//    void evaluate() {
//        EventSymbol es1 = new EventSymbol("A_Accepted",0,"");
//        EventSymbol es2 = new EventSymbol("A_Create Application",1,"");
//        EventSymbol es3 = new EventSymbol("A_Concept",2,"");
//        EventSymbol es4 = new EventSymbol("A_Submitted",3,"");
//        List<EventSymbol> events = new ArrayList<>();
//        events.add(es1);
//        events.add(es2);
//        events.add(es3);
//        events.add(es4);
//
//        List<Constraint> lc = new ArrayList<>();
//        lc.add(new GapConstraint(1,2,10));
//        ComplexPattern p = new ComplexPattern(events);
//        p.setConstraints(lc);
//
//        Metadata m = dbConnector.getMetadata("bpi_2017");
//        QueryPatternDetectionWrapper queryPatternDetectionWrapper = new QueryPatternDetectionWrapper();
//        queryPatternDetectionWrapper.setPattern(p);
//        queryPatternDetectionWrapper.setLog_name("bpi_2017");
//        QueryPlan queryPlan =qpd.createQueryPlan(queryPatternDetectionWrapper,m);
//        queryPlan.setMetadata(m);
//        queryPlan.execute(queryPatternDetectionWrapper);
//        QueryPlanPatternDetection queryPlan1 =(QueryPlanPatternDetection)queryPlan;
//        IndexMiddleResult imr = queryPlan1.getImr();
//
//        List<Occurrences> matches = saseConnector.evaluate(p.getItSimpler(),imr.getEvents(),false);
//    }

    private Stream getStreamTimestamp(){
        Stream s = new Stream(6);
        List<UncertainTimeEvent> l = new ArrayList<>(){{
            add(new UncertainTimeEvent("1",1,"A",1,true,0));
            add(new UncertainTimeEvent("1",2,"A",4,true,3));
            add(new UncertainTimeEvent("1",3,"B",7,true,5));
            add(new UncertainTimeEvent("1",4,"B",12,true,0));
            add(new UncertainTimeEvent("1",5,"B",13,true,1));
            add(new UncertainTimeEvent("1",6,"C",18,true,0));
        }};
        s.setEvents(l.toArray(new UncertainTimeEvent[6]));
        return s;
    }

    @Test
    void testParsing(){
//        State s = new State(1,"","A","normal");
//        s.addPredicate("$3.change <= 5 - $1.change + $2.change");

        EngineController ec = new EngineController();
        List<String> query = new ArrayList<>(){{
            add("PATTERN SEQ(A a1, B b1, C c1)");
            add("WHERE skip-till-next-match");
            add("AND  b1.timestamp <= -12 + a1.timestamp");
//            add("AND c.change < (3 + a.change + b.change)");
            add("WITHIN 100");
            add("RETURN a,b,c");
        }};
        List<Event> events = new ArrayList<>(){{
            add(new EventPos("A",1));
            add(new EventPos("B",2));
            add(new EventPos("C",3));
        }};

        State[] states = new State[events.size()];
        for(int i = 0; i<events.size();i++){
            State s = new State(i+1,"a",String.format("%s",events.get(i).getName()),"normal");
            states[i] = s;
        }
//        states[1].addPredicate("timestamp <= $previous.timestamp + 10");
        states[2].addPredicate("change <=  3 - $2.change - $1.change  ");
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-any-match");
        nfaWrapper.setSize(3);
        nfaWrapper.setStates(states);


        ec.setNfa(new NFA(nfaWrapper));
        ec.initializeEngine();
        ec.setInput(this.getStreamTimestamp());
        try {
            ec.runEngine();
        } catch (CloneNotSupportedException | EvaluationException e) {
            throw new RuntimeException(e);
        }
        List<Match> m = ec.getMatches();
        Assertions.assertTrue(true);

    }

}