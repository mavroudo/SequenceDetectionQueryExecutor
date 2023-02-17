package com.datalab.siesta.queryprocessor.model;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseConnector;
import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraint;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraint;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Events.EventTs;
import com.datalab.siesta.queryprocessor.model.Patterns.SimplePattern;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootTest
class PossibleOrderOfEventsTest {

    @Autowired
    private SaseConnector saseConnector;

    @Test
    void getPossibleOrderOfEvents() {
        List<Event> events = new ArrayList<>();
        events.add(new EventTs("A", new Timestamp(10000)));
        events.add(new EventTs("B", new Timestamp(12000)));
        events.add(new EventTs("C", new Timestamp(15000)));
        List<Constraint> constraints = new ArrayList<>();
        constraints.add(new TimeConstraint(0, 1, 1));
        PossibleOrderOfEvents porder = new PossibleOrderOfEvents(0, events, constraints, 3);
        List<PossibleOrderOfEvents> porders = porder.getPossibleOrderOfEvents();
        Assertions.assertEquals(3, porders.size());


        List<Event> events2 = new ArrayList<>();
        events2.add(new EventPos("A", 0));
        events2.add(new EventPos("B", 2));
        events2.add(new EventPos("C", 5));
        List<Constraint> constraints2 = new ArrayList<>();
        constraints2.add(new GapConstraint(0, 1, 3));
        PossibleOrderOfEvents porder2 = new PossibleOrderOfEvents(0, events, constraints2, 3);
        List<PossibleOrderOfEvents> porders2 = porder.getPossibleOrderOfEvents();
        Assertions.assertEquals(3, porders2.size());

        //test empty constraints
        List<Constraint> constraints3 = new ArrayList<>();
        PossibleOrderOfEvents porder3 = new PossibleOrderOfEvents(0, events, constraints3, 3);
        List<PossibleOrderOfEvents> porders3 = porder3.getPossibleOrderOfEvents();
        Assertions.assertEquals(1, porders3.size());

    }


    @Test
    void evaluatePatternConstraints() {

        SimplePattern sp = new SimplePattern();
        List<EventPos> ep = new ArrayList<>() {{
            add(new EventPos("A", 0));
            add(new EventPos("B", 1));
            add(new EventPos("C", 2));
        }};
        List<Event> e1 = new ArrayList<>() {{
            add(new EventTs("A", new Timestamp(10000)));
            add(new EventTs("B", new Timestamp(12000)));
            add(new EventTs("C", new Timestamp(20000)));
        }};

        List<Event> e2 = new ArrayList<>() {{
            add(new EventTs("A", new Timestamp(10000)));
            add(new EventTs("B", new Timestamp(17000)));
            add(new EventTs("C", new Timestamp(20000)));
        }};

        List<Event> e3 = new ArrayList<>() {{
            add(new EventTs("A", new Timestamp(10000)));
            add(new EventTs("C", new Timestamp(17000)));
            add(new EventTs("B", new Timestamp(20000)));
        }};
        List<Constraint> cs = new ArrayList<>() {{
            add(new TimeConstraint(0, 1, 1));
            add(new TimeConstraint(1, 2, 2));
        }};
        sp.setEvents(ep);
        sp.setConstraints(cs);

        PossibleOrderOfEvents p1 = new PossibleOrderOfEvents(1, e1, cs, 5);
        PossibleOrderOfEvents p2 = new PossibleOrderOfEvents(2, e2, cs, 5);
        PossibleOrderOfEvents p3 = new PossibleOrderOfEvents(3, e3, cs, 5);
        List<PossibleOrderOfEvents> porder = new ArrayList<>();
        porder.addAll(p1.getPossibleOrderOfEvents());
        porder.addAll(p2.getPossibleOrderOfEvents());
        porder.addAll(p3.getPossibleOrderOfEvents());

        Map<Long,List<Event>> map = new HashMap<>(){{
            put(1L,e1);
            put(2L,e2);
            put(3L,e3);
        }};

        List<Occurrences> first = saseConnector.evaluate(sp,map,false); //nothing will be found
        Assertions.assertEquals(0,first.size());


        List<OccurrencesWhyNotMatch> s = saseConnector.evaluate(sp, porder, true);
        Assertions.assertEquals(4,s.size());

        //The four previous possible order will go under evaluation
        s.get(0).evaluateConstraints();


        System.out.println("hey");
    }
}