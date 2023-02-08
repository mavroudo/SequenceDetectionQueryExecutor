package com.datalab.siesta.queryprocessor.SaseConnection;

import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraint;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraint;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexMiddleResult;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.Events.EventSymbol;
import com.datalab.siesta.queryprocessor.model.Occurrences;
import com.datalab.siesta.queryprocessor.model.Patterns.ComplexPattern;
import com.datalab.siesta.queryprocessor.model.Patterns.SimplePattern;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlanPatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.QueryPatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import edu.umass.cs.sase.engine.Match;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class SaseConnectorTest {

    @Autowired
    private SaseConnector saseConnector;

    @Autowired
    private DBConnector dbConnector;

    @Autowired
    private QueryPatternDetection qpd;

    @Test
    void evaluate() {
        EventSymbol es1 = new EventSymbol("A_Accepted",0,"");
        EventSymbol es2 = new EventSymbol("A_Create Application",1,"");
        EventSymbol es3 = new EventSymbol("A_Concept",2,"");
        EventSymbol es4 = new EventSymbol("A_Submitted",3,"");
        List<EventSymbol> events = new ArrayList<>();
        events.add(es1);
        events.add(es2);
        events.add(es3);
        events.add(es4);

        List<Constraint> lc = new ArrayList<>();
        lc.add(new GapConstraint(1,2,10));
        ComplexPattern p = new ComplexPattern(events);
        p.setConstraints(lc);

        Metadata m = dbConnector.getMetadata("bpi_2017");
        QueryPatternDetectionWrapper queryPatternDetectionWrapper = new QueryPatternDetectionWrapper();
        queryPatternDetectionWrapper.setPattern(p);
        queryPatternDetectionWrapper.setLog_name("bpi_2017");
        QueryPlan queryPlan =qpd.createQueryPlan(queryPatternDetectionWrapper,m);
        queryPlan.setMetadata(m);
        queryPlan.execute(queryPatternDetectionWrapper);
        QueryPlanPatternDetection queryPlan1 =(QueryPlanPatternDetection)queryPlan;
        IndexMiddleResult imr = queryPlan1.getImr();

        List<Occurrences> matches = saseConnector.evaluate(p.getItSimpler(),imr.getEvents(),false);

        System.out.println("hey");







    }
}