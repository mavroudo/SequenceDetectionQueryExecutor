package com.datalab.siesta.queryprocessor.model.WhyNotMatch.UsingSase;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseConnector;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Patterns.SIESTAPattern;
import com.datalab.siesta.queryprocessor.model.Patterns.SimplePattern;
import edu.umass.cs.sase.engine.EngineController;
import edu.umass.cs.sase.engine.Match;
import edu.umass.cs.sase.query.NFA;
import edu.umass.cs.sase.stream.Stream;
import net.sourceforge.jeval.EvaluationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class WhyNotMatchSASE {

    public WhyNotMatchSASE() {
    }

    public void evaluate(SimplePattern sp,Map<Long, List<Event>> restEvents, int uncertaintyPerEvent, int k){
        NFA nfa = this.getNFA(sp);
        Map<Long,List<Match>> matches = restEvents.entrySet().stream().parallel().map(entry->{
            Stream s = this.getUnCertainStream(entry.getKey(),entry.getValue(),uncertaintyPerEvent);
            EngineController ec = new EngineController();
            ec.setNfa(nfa);
            ec.setInput(s);
            ec.initializeEngine();
            try {
                ec.runEngine();
            } catch (CloneNotSupportedException | EvaluationException e) {
                throw new RuntimeException(e);
            }
            return new Tuple2<>(entry.getKey(), ec.getMatches());
        }).filter(x->!x._2.isEmpty())
                .collect(Collectors.toMap(Tuple2::_1,Tuple2::_2));
        this.createResponse(matches);
    }

    private Stream getUnCertainStream(long trace_id, List<Event> events, int uncertaintyPerEvent){
        Stream s = new Stream(10);
        return s;
    }

    private NFA getNFA(SimplePattern sp){

        return new NFA("");
    }

    public void createResponse(Map<Long,List<Match>> maps){

    }



}
