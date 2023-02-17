package com.datalab.siesta.queryprocessor.model.WhyNotMatch.UsingSase;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraint;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Events.EventTs;
import com.datalab.siesta.queryprocessor.model.Patterns.SimplePattern;
import edu.umass.cs.sase.query.NFA;
import edu.umass.cs.sase.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class WhyNotMatchSASETest {

    @Autowired
    private WhyNotMatchSASE whyNotMatchSASE;

    @Test
    void testGetUncertainStream() {
        List<Event> events =getEvents();
        Stream s = whyNotMatchSASE.getUnCertainStream(1, events, 3);
        Assertions.assertEquals(3 * events.size(), s.getSize());
    }

    private List<Event> getEvents(){
        List<Event> events = new ArrayList<>() {{
            add(new EventTs("A", new Timestamp(101000)));
            add(new EventTs("B", new Timestamp(104000)));
            add(new EventTs("C", new Timestamp(109000)));
        }};
        return  events;
    }

    private SimplePattern getSp(){
        SimplePattern sp = new SimplePattern();
        List<Constraint> constraints = new ArrayList<>(){{
            add(new TimeConstraint(0,1,2));
            TimeConstraint t = new TimeConstraint(1,2,7);
            t.setMethod("atleast");
            add(t);
        }};
        List<EventPos> events = new ArrayList<>(){{
            add(new EventPos("A", 0));
            add(new EventPos("B", 1));
            add(new EventPos("C", 2));
        }};
        sp.setConstraints(constraints);
        sp.setEvents(events);
        return sp;
    }
    @Test
    void testGetNFA(){
        SimplePattern sp = this.getSp();
        NFA nfa = whyNotMatchSASE.getNFA(sp,3);
        Assertions.assertNotNull(nfa);
    }

    @Test
    void testEvaluation(){
        Map<Long,List<Event>> restEvents = new HashMap<>();
        restEvents.put(1L,this.getEvents());
        whyNotMatchSASE.evaluate(this.getSp(),restEvents,3,3);
    }



}