package com.datalab.siesta.queryprocessor.SetContainment;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseConnector;
import com.datalab.siesta.queryprocessor.Signatures.Signature;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Occurrences;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponsePatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.model.TimeStats;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QuerySetContainmentPlan implements QueryPlan {

    private CassandraConnectionSetContainment cassandraConnectionSetContainment;

    private SaseConnector saseConnector;

    @Autowired
    public QuerySetContainmentPlan(CassandraConnectionSetContainment cassandraConnectionSetContainment,
                                   SaseConnector saseConnector){
        this.cassandraConnectionSetContainment=cassandraConnectionSetContainment;
        this.saseConnector=saseConnector;
    }

    @Override
    public QueryResponse execute(QueryWrapper qw) {
        long start = System.currentTimeMillis();
        QueryPatternDetectionWrapper qpdw = (QueryPatternDetectionWrapper) qw;
        List<Long> possibleTraces = cassandraConnectionSetContainment.getPossibleTraceIds(qpdw.getPattern(),
                qpdw.getLog_name());
        Map<Long,List<Event>> originalTraces = cassandraConnectionSetContainment.getOriginalTraces(possibleTraces,
                qpdw.getLog_name());
        long ts_trace = System.currentTimeMillis();
        List<Occurrences> occurrences = saseConnector.evaluate(qpdw.getPattern(),originalTraces,false);
        occurrences.forEach(x->x.clearOccurrences(qpdw.isReturnAll()));
        long ts_eval = System.currentTimeMillis();
        QueryResponsePatternDetection queryResponsePatternDetection = new QueryResponsePatternDetection();
        queryResponsePatternDetection.setOccurrences(occurrences);
        TimeStats timeStats = new TimeStats();
        timeStats.setTimeForPrune(ts_trace-start);
        timeStats.setTimeForValidation(ts_eval-ts_trace);
        timeStats.setTotalTime(ts_eval-start);
        queryResponsePatternDetection.setTimeStats(timeStats);
        return queryResponsePatternDetection;
    }

    @Override
    public void setMetadata(Metadata metadata) {
        //no need for metadata
    }
}
