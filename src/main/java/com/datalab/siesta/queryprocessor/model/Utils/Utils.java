package com.datalab.siesta.queryprocessor.model.Utils;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseEvent;
import com.datalab.siesta.queryprocessor.model.Constraints.*;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Events.EventTs;
import org.apache.spark.broadcast.Broadcast;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Contains some general functions that are utilized throughout the project and since it is a component it is easy
 * to call them from anywhere
 */
@Component
public class Utils implements Serializable {


    /**
     * Separates a list of constraints into 2 lists (1) the Time constraints and (2) the gap constaints
     * @param constraints a list of all the constraints in the query pattern
     * @return a tuple of the 2 lists of constraints separated
     */
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


    /**
     * Transform a list of Siesta events into a list of SASE events in order to be evaluated by SASE engine
     * @param events a list of Siesta events
     * @return the transformed SASE events
     */
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

    public boolean evaluateEvent(Event e, Broadcast<Timestamp> bFrom, Broadcast<Timestamp> bTill) {
        if (e instanceof EventPos) return true;
        else {
            EventTs et = (EventTs) e;
            if (bFrom.value() != null & bTill.value() != null)
                return !et.getTimestamp().before(bFrom.value()) && !et.getTimestamp().after(bTill.value());
            else if (bFrom.value() != null)
                return !et.getTimestamp().before(bFrom.value());
            else if (bTill.value()!=null)
                return !et.getTimestamp().after(bTill.value());
            else return true;
        }
    }
}
