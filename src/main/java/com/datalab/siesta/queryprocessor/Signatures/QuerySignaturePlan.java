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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

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
        QueryPatternDetectionWrapper qpdw = (QueryPatternDetectionWrapper) qw;
        Signature signature = cassandraConnectionSignature.getSignature(qpdw.getLog_name());
        List<Long> possibleTraces = cassandraConnectionSignature.getPossibleTraceIds(qpdw.getPattern(),
                qpdw.getLog_name(),signature);
        Map<Long,List<Event>> originalTraces = cassandraConnectionSignature.getOriginalTraces(possibleTraces,
                qpdw.getLog_name());
        List<Occurrences> occurrences = saseConnector.evaluate(qpdw.getPattern(),originalTraces,false);
        occurrences.forEach(x->x.clearOccurrences(qpdw.isReturnAll()));
        QueryResponsePatternDetection queryResponsePatternDetection = new QueryResponsePatternDetection();
        queryResponsePatternDetection.setOccurrences(occurrences);
        return queryResponsePatternDetection;
    }

    @Override
    public void setMetadata(Metadata metadata) {
        //no need for metadata
    }
}
