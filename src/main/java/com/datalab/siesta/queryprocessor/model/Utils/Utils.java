package com.datalab.siesta.queryprocessor.model.Utils;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseEvent;
import com.datalab.siesta.queryprocessor.model.Constraints.*;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Events.EventTs;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class Utils {

    public Tuple2<List<TimeConstraintWE>, List<GapConstraintWE>> splitConstraints(Set<EventPair> pairs) {
        List<TimeConstraintWE> tcs = new ArrayList<>();
        List<GapConstraintWE> gcs = new ArrayList<>();
        for (EventPair p : pairs) {
            if (p.getConstraint() != null) {
                if (p.getConstraint() instanceof TimeConstraint) tcs.add(new TimeConstraintWE(p));
                if (p.getConstraint() instanceof GapConstraint) gcs.add(new GapConstraintWE(p));
            }
        }
        return new Tuple2<>(tcs, gcs);
    }

    public Tuple2<List<TimeConstraint>, List<GapConstraint>> splitConstraints(List<Constraint> constraints){
        List<TimeConstraint> tcs = new ArrayList<>();
        List<GapConstraint> gcs = new ArrayList<>();
        for(Constraint c: constraints){
            if (c != null) {
                if (c instanceof TimeConstraint) tcs.add((TimeConstraint) c);
                if (c instanceof GapConstraint) gcs.add((GapConstraint) c);
            }
        }
        return new Tuple2<>(tcs, gcs);
    }


    public List<SaseEvent> transformToSaseEvents(List<Event> events){
        List<SaseEvent> ses = new ArrayList<>();
        Event fe = events.get(0);
        if(fe instanceof EventTs){ // handling events ts
            long minTs = ((EventTs) fe).getTimestamp().getTime();
            SaseEvent se = new SaseEvent((int)fe.getTraceID(),0,fe.getName(),0,true);
            se.setMinTs(minTs);
            ses.add(se);
            for(int i =1 ;i<events.size();i++){
                ses.add(((EventTs)events.get(i)).transformSaseEvent(i,minTs));
            }
        }else if(fe instanceof EventPos){ //handling event positions
            for(int i=0;i<events.size();i++){
                ses.add(events.get(i).transformSaseEvent(i));
            }
        }
        return  ses;
    }
}
