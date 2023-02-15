package com.datalab.siesta.queryprocessor.model;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraint;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraint;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Events.EventTs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PossibleOrderOfEventsTest {

    @Test
    void getPossibleOrderOfEvents() {
        List<Event> events = new ArrayList<>();
        events.add(new EventTs("A",new Timestamp(10000)));
        events.add(new EventTs("B",new Timestamp(12000)));
        events.add(new EventTs("C",new Timestamp(15000)));
        List<Constraint> constraints = new ArrayList<>();
        constraints.add(new TimeConstraint(0,1,1));
        PossibleOrderOfEvents porder = new PossibleOrderOfEvents(0,events,constraints,3);
        List<PossibleOrderOfEvents> porders = porder.getPossibleOrderOfEvents();
        Assertions.assertEquals(3,porders.size());


        List<Event> events2 = new ArrayList<>();
        events2.add(new EventPos("A",0));
        events2.add(new EventPos("B",2));
        events2.add(new EventPos("C",5));
        List<Constraint> constraints2 = new ArrayList<>();
        constraints2.add(new GapConstraint(0,1,3));
        PossibleOrderOfEvents porder2 = new PossibleOrderOfEvents(0,events,constraints2,3);
        List<PossibleOrderOfEvents> porders2 = porder.getPossibleOrderOfEvents();
        Assertions.assertEquals(3,porders2.size());

        //test empty constraints
        List<Constraint> constraints3 = new ArrayList<>();
        PossibleOrderOfEvents porder3 = new PossibleOrderOfEvents(0,events,constraints3,3);
        List<PossibleOrderOfEvents> porders3 = porder3.getPossibleOrderOfEvents();
        Assertions.assertEquals(1,porders3.size());

    }



    @Test
    void evaluatePatternConstraints() {
    }
}