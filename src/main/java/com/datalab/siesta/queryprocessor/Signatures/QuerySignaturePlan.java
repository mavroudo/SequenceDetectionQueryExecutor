package com.datalab.siesta.queryprocessor.Signatures;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseConnector;
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
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;


/**
 * Contains the logic for how the signatures query is able to utilize the stored indice in order to respond to
 * pattern detection queries
 */
@Configuration
@ConditionalOnProperty(
        value = "database",
        havingValue = "cassandra-rdd"
)
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QuerySignaturePlan implements QueryPlan {

    private CassandraConnectionSignature cassandraConnectionSignature;

    private SaseConnector saseConnector;

    @Autowired
    public QuerySignaturePlan(CassandraConnectionSignature cassandraConnectionSignature, SaseConnector saseConnector) {
        this.cassandraConnectionSignature = cassandraConnectionSignature;
        this.saseConnector = saseConnector;
    }

    @Override
    public QueryResponse execute(QueryWrapper qw) {
        long start = System.currentTimeMillis();
        QueryPatternDetectionWrapper qpdw = (QueryPatternDetectionWrapper) qw;
        Signature signature = cassandraConnectionSignature.getSignature(qpdw.getLog_name());
        List<Long> possibleTraces = cassandraConnectionSignature.getPossibleTraceIds(qpdw.getPattern(),
                qpdw.getLog_name(),signature);
        Map<Long,List<Event>> originalTraces = cassandraConnectionSignature.getOriginalTraces(possibleTraces,
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
