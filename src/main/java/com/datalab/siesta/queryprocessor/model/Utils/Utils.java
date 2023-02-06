package com.datalab.siesta.queryprocessor.model.Utils;

import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraint;
import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraintWE;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraint;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraintWE;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
}
