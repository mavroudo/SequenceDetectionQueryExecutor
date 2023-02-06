package com.datalab.siesta.queryprocessor.model.Queries.QueryEvaluator;

import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraintWE;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraintWE;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Occurrence;
import com.datalab.siesta.queryprocessor.model.Patterns.SimplePattern;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class Evaluator {

    protected DBConnector dbConnector;

    protected Utils utils;

    private Metadata metadata;

    public Map<Long, List<Occurrence>> evaluate(List<Long> trace_ids, Map<Long, List<Event>> gathered, SimplePattern pattern,
                                                List<TimeConstraintWE> tcs, List<GapConstraintWE> gcs, String logname) {
        Map<Long, List<EventBoth>> traces = this.getRequiredData(trace_ids, gathered, pattern, tcs, gcs,logname);
        return this.getOccurrencesPerTrace(traces, pattern, tcs, gcs);
    }

    protected Map<Long, List<EventBoth>> getRequiredData(List<Long> trace_ids, Map<Long, List<Event>> gathered, SimplePattern pattern,
                                                         List<TimeConstraintWE> tcs, List<GapConstraintWE> gcs,
                                                         String logname) {
        if ((!tcs.isEmpty() && !gcs.isEmpty()) || (!gcs.isEmpty() && metadata.getMode().equals("timestamps")) ||
                (!tcs.isEmpty() && metadata.getMode().equals("positions"))) {
            List<String> eventTypes = pattern.getEventTypes();
            return dbConnector.querySeqTable(logname,trace_ids,eventTypes);
        } else {
            Map<Long,List<EventBoth>> result = new HashMap<>();
            for(Map.Entry<Long,List<Event>> e: gathered.entrySet()){
                result.put(e.getKey(),e.getValue().stream().map(Event::getEventBoth).collect(Collectors.toList()));
            }
            return result;
        }
    }

    protected Map<Long, List<Occurrence>> getOccurrencesPerTrace(Map<Long, List<EventBoth>> traces, SimplePattern pattern,
                                                                 List<TimeConstraintWE> tcs, List<GapConstraintWE> gcs) {
        return null;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }
}
