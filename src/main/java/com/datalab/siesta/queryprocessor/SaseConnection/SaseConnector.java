package com.datalab.siesta.queryprocessor.SaseConnection;

import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Patterns.SimplePattern;
import edu.umass.cs.sase.engine.EngineController;
import edu.umass.cs.sase.engine.Match;
import edu.umass.cs.sase.query.NFA;

import edu.umass.cs.sase.stream.Stream;
import net.sourceforge.jeval.EvaluationException;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SaseConnector {


    public void evaluate(SimplePattern pattern, Map<Long,List<Event>> events){
        EngineController ec = this.getEngineController(pattern);
        ec.initializeEngine();
        Stream s = this.getStream(events.values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList()));
        ec.setInput(s);
        try {
            ec.runEngine();
        } catch (CloneNotSupportedException | EvaluationException e) {
            throw new RuntimeException(e);
        }
        List<Match> matches = ec.getMatches();
        System.out.println("hey");

    }

    private EngineController getEngineController(SimplePattern pattern){
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(pattern.getEvents().size());
        nfaWrapper.setStates(pattern.getNfa());
//        nfaWrapper.setPartitionAttribute("trace_id"); //? TODO: check this out
        ec.setNfa(new NFA(nfaWrapper));
        return ec;
    }


    private Stream getStream(List<Event> events){
        Stream s = new Stream(events.size());
        List<SaseEvent> saseEvents = events.stream().map(Event::transformSaseEvent).collect(Collectors.toList());
        s.setEvents(saseEvents.toArray(new SaseEvent[events.size()]));
        return s;
    }



}
