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

import java.util.*;
import java.util.stream.Collectors;

@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SaseConnector {


    public Map<Long,List<Match>> evaluate(SimplePattern pattern, Map<Long,List<Event>> events){
        EngineController ec = this.getEngineController(pattern);
        Map<Long,List<Match>> results = new HashMap<>();
        for(Map.Entry<Long,List<Event>> e: events.entrySet()){
            ec.initializeEngine();
            Stream s = this.getStream(new ArrayList<>(e.getValue()));
            ec.setInput(s);
            try {
                ec.runEngine();
            } catch (CloneNotSupportedException | EvaluationException exe) {
                throw new RuntimeException(exe);
            }
            if(!ec.getMatches().isEmpty()) results.put(e.getKey(),ec.getMatches());
        }
        return results;
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
        List<SaseEvent> saseEvents = new ArrayList<>();
        for(int i=0;i<events.size();i++){
            saseEvents.add(events.get(i).transformSaseEvent(i));
        }
        s.setEvents(saseEvents.toArray(new SaseEvent[events.size()]));
        return s;
    }



}
