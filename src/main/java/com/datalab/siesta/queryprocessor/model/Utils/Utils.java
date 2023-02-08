package com.datalab.siesta.queryprocessor.model.Utils;

import com.datalab.siesta.queryprocessor.model.Constraints.*;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
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
}
