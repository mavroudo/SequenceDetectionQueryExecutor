package com.datalab.siesta.queryprocessor.model.WhyNotMatch.UsingSase;

import com.datalab.siesta.queryprocessor.SaseConnection.NFAWrapper;
import com.datalab.siesta.queryprocessor.SaseConnection.SaseConnector;
import com.datalab.siesta.queryprocessor.SaseConnection.SaseEvent;
import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraint;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraint;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Patterns.SIESTAPattern;
import com.datalab.siesta.queryprocessor.model.Patterns.SimplePattern;
import com.datalab.siesta.queryprocessor.model.WhyNotMatch.AlmostMatch;
import edu.umass.cs.sase.engine.EngineController;
import edu.umass.cs.sase.engine.Match;
import edu.umass.cs.sase.query.NFA;
import edu.umass.cs.sase.query.State;
import edu.umass.cs.sase.stream.Stream;
import net.sourceforge.jeval.EvaluationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import scala.collection.mutable.ListBuffer;

import java.util.*;
import java.util.stream.Collectors;

@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class WhyNotMatchSASE {

    public WhyNotMatchSASE() {
    }

    public List<AlmostMatch> evaluate(SimplePattern sp, Map<Long, List<Event>> restEvents, int uncertaintyPerEvent, int k) {
        NFA nfa = this.getNFA(sp,k);
        Map<Long, List<Match>> matches = restEvents.entrySet().stream().parallel().map(entry -> {
                    Stream s = this.getUnCertainStream(entry.getKey(), entry.getValue(), uncertaintyPerEvent);
                    EngineController ec = new EngineController();
                    ec.setNfa(nfa);
                    ec.initializeEngine();
                    ec.setInput(s);
                    try {
                        ec.runEngine();
                    } catch (CloneNotSupportedException | EvaluationException e) {
                        throw new RuntimeException(e);
                    }
                    return new Tuple2<>(entry.getKey(), ec.getMatches());
                }).filter(x -> !x._2.isEmpty())
                .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
        return this.createResponse(matches,restEvents);
    }

    /**
     * @param trace_id            the id of the trace we are handling
     * @param events              a list of events retrieved from the database (can be EventBoth, EventTs and EventPos)
     * @param uncertaintyPerEvent the range that a timestamp or position can move
     * @return a Stream of the events to be handled by the Sase Engine
     */
    public Stream getUnCertainStream(long trace_id, List<Event> events, int uncertaintyPerEvent) {
        List<UncertainTimeEvent> eventBuffer = new ArrayList<>();
        for (Event e : events) {
            long original = e.getPrimaryMetric();
            long minimum = Math.max(original - uncertaintyPerEvent, 0);
            long maximum = original + uncertaintyPerEvent;
            for (long i = minimum; i <= maximum; i++) {
                eventBuffer.add(new UncertainTimeEvent(trace_id, 0,
                        e.getName(), (int) i, true, (int) Math.abs(original - i)));
            }
        }
        Stream s = new Stream(eventBuffer.size());
        Collections.sort(eventBuffer);
        for (int i = 0; i < eventBuffer.size(); i++) {
            eventBuffer.get(i).setId(i);
            eventBuffer.get(i).setPosition(i);
        }
        s.setEvents(eventBuffer.toArray(new UncertainTimeEvent[0]));
        return s;
    }

    /**
     * Return the NFA that will be used in the SASE engine in order to detect why not match matches
     * @param sp A simple pattern
     * @param k The maximum number of changes allowed
     * @return the NFA
     */
    public NFA getNFA(SimplePattern sp, int k) {
        int states_size = sp.getEvents().size();
        State[] states = new State[states_size];
        for (int i = 0; i < sp.getEvents().size(); i++) {
            State s = new State(i + 1, "", String.format("%s", sp.getEvents().get(i).getName()), "normal");
            this.generatePredicates(i, sp.getConstraints()).forEach(s::addPredicate); // add the time-constraints/gap constraints here
            states[i] = s;
        }
        List<String> changes = new ArrayList<>() {{ //add the limitation for the k here
            for (int i = 0; i < states_size - 1; i++) {
                add(String.format(" - $%d.change", i + 1));
            }
        }};
        String pDescription = String.format("change <= %d ", k) + String.join(" ", changes);
        states[states_size - 1].addPredicate(pDescription);
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-any-match");
        nfaWrapper.setSize(sp.getSize());
        nfaWrapper.setStates(states);
        return new NFA(nfaWrapper);
    }

    /**
     * Generate the strings that dictates the predicates. i value is equal to state number -1, and that is because
     * states starts from 1 and states[] startrs from 0
     *
     * @param i equal to the number of state -1
     * @return a list of the preicates for this state
     */
    private List<String> generatePredicates(int i, List<Constraint> constraints) {
        List<String> response = new ArrayList<>();
        for (Constraint c : constraints) {
            if (c.getPosB() == i && c instanceof GapConstraint) {
                if (i > 0) response.add("position > $previous.position");
                GapConstraint gc = (GapConstraint) c;
                if (gc.getMethod().equals("within"))
                    response.add(String.format(" position <= $%d.position + %d ", c.getPosA() + 1, gc.getConstraint()));
                else
                    response.add(String.format(" position >= $%d.position + %d ", c.getPosA() + 1, gc.getConstraint())); //atleast
            } else if (c.getPosB() == i && c instanceof TimeConstraint) {
                TimeConstraint tc = (TimeConstraint) c;
                if (i > 0) response.add("timestamp > $previous.timestamp");
                if (tc.getMethod().equals("within"))
                    response.add(String.format(" timestamp <= $%d.timestamp + %d ", c.getPosA() + 1, tc.getConstraint()));
                else
                    response.add(String.format(" timestamp >= $%d.timestamp + %d ", c.getPosA() + 1, tc.getConstraint())); //atleast
            }
        }
        return response;
    }

    public List<AlmostMatch> createResponse(Map<Long, List<Match>> maps, Map<Long,List<Event>> original) {
        return maps.entrySet().stream().map(entry->{
            List<UncertainTimeEvent> e = entry.getValue().stream().map(x -> {
                        List<UncertainTimeEvent> u = new ArrayList<>() {{
                            Arrays.stream(x.getEvents()).forEach(t -> add((UncertainTimeEvent) t));
                        }};
                        int sum = u.stream().mapToInt(UncertainTimeEvent::getChange).sum();
                        return new Tuple2<>(u, sum);
                    }).reduce((x, y) -> x._2 < y._2 ? x : y)
                    .map(x -> x._1).get();
                return  new AlmostMatch(entry.getKey(),original.get(entry.getKey()),e);
        }).collect(Collectors.toList());
    }
}
