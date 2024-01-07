package com.datalab.siesta.queryprocessor.SaseConnection;

import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.GroupOccurrences;
import com.datalab.siesta.queryprocessor.model.Occurrence;
import com.datalab.siesta.queryprocessor.model.Occurrences;
import com.datalab.siesta.queryprocessor.model.Patterns.SIESTAPattern;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import edu.umass.cs.sase.engine.EngineController;
import edu.umass.cs.sase.engine.Match;
import edu.umass.cs.sase.query.NFA;
import edu.umass.cs.sase.stream.Stream;
import net.sourceforge.jeval.EvaluationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class describes the connection between SIESTA and SASE.
 */
@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SaseConnector {


    private Utils utils;

    @Autowired
    public SaseConnector(Utils utils){
        this.utils=utils;
    }

    /**
     * For a given pattern and a list of traces, returns where this pattern occurred
     * @param pattern the user defined pattern
     * @param events the required events for each trace in order to determine if pattern occurs
     * @param onlyAppearances set to false if the pattern contains no constraints
     * @return where the pattern occur
     */
    public List<Occurrences> evaluate(SIESTAPattern pattern, Map<Long, List<Event>> events, boolean onlyAppearances) {
        EngineController ec = this.getEngineController(pattern, onlyAppearances);
        List<Occurrences> occurrences = new ArrayList<>();
        for (Map.Entry<Long, List<Event>> e : events.entrySet()) {
            ec.initializeEngine();
            if(e.getValue().isEmpty()){ // in case that events have been removed due to filters
                continue;
            }
            Stream s = this.getStream(new ArrayList<>(e.getValue()));
            ec.setInput(s);
            try {
                ec.runEngine();
            } catch (CloneNotSupportedException | EvaluationException exe) {
                throw new RuntimeException(exe);
            }
            if (!ec.getMatches().isEmpty()) {
                Occurrences ocs = new Occurrences();
                ocs.setTraceID(e.getKey());
                for (Match m : ec.getMatches()) {
                    ocs.addOccurrence(new Occurrence(Arrays.stream(m.getEvents()).parallel()
                            .map(x -> (SaseEvent) x)
                            .map(SaseEvent::getEventBoth)
                            .collect(Collectors.toList())));
                }
                occurrences.add(ocs);
            }
        }
        return occurrences;
    }

    /**
     * Similar to the above method but it is used to evaluate the appearence of a pattern in the events when they
     * are grouped for different trace-groups
     * @param pattern the user defined pattern
     * @param events the required events for each trace in order to determine if pattern occurs
     * @return where the pattern occur
     */
    public List<GroupOccurrences> evaluateGroups(SIESTAPattern pattern, Map<Integer, List<EventBoth>> events){
        EngineController ec = this.getEngineController(pattern,false);
        List<GroupOccurrences> occurrences = new ArrayList<>();
        for (Map.Entry<Integer, List<EventBoth>> e : events.entrySet()) {
            ec.initializeEngine();
            Stream s = this.getStream(new ArrayList<>(e.getValue()));
            ec.setInput(s);
            try {
                ec.runEngine();
            } catch (CloneNotSupportedException | EvaluationException exe) {
                throw new RuntimeException(exe);
            }
            if (!ec.getMatches().isEmpty()) {
                GroupOccurrences ocs = new GroupOccurrences();
                ocs.setGroupId(e.getKey());
                for (Match m : ec.getMatches()) {
                    ocs.addOccurrence(new Occurrence(Arrays.stream(m.getEvents()).parallel()
                            .map(x -> (SaseEvent) x)
                            .map(SaseEvent::getEventBoth)
                            .collect(Collectors.toList())));
                }
                occurrences.add(ocs);
            }
        }
        return occurrences;
    }

    /**
     * Based on the pattern it creates a NFA that contains the states and the transitions of a state machine
     * that will be used to detect the occurrences of the pattern
     * @param pattern the user defined pattern
     * @param onlyAppearances set to false if the pattern contains no constraints
     * @return the egnine that will be used for the evaluation
     */
    private EngineController getEngineController(SIESTAPattern pattern, boolean onlyAppearances) {
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(pattern.getSize());
        if (onlyAppearances) {
            nfaWrapper.setStates(pattern.getNfaWithoutConstraints());
        } else {
            nfaWrapper.setStates(pattern.getNfa());
        }
        nfaWrapper.setSize(nfaWrapper.getStates().length);
        ec.setNfa(new NFA(nfaWrapper));
        return ec;
    }


    /**
     * Transforms the retrieved events from a trace to a stream (as described in SASE)
     * @param events the retrieved events of a trace
     * @return the events transformed in stream
     */
    private Stream getStream(List<Event> events) {
        Stream s = new Stream(events.size());
        List<SaseEvent> saseEvents = utils.transformToSaseEvents(events);
        s.setEvents(saseEvents.toArray(new SaseEvent[events.size()]));
        return s;
    }


}
