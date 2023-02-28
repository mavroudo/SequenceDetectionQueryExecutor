package com.datalab.siesta.queryprocessor.Signatures;

import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.Query;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

@Configuration
@ConditionalOnProperty(
        value = "database",
        havingValue = "cassandra-rdd"
)
@Service
public class QuerySignatures implements Query {

    private QuerySignaturePlan querySignaturePlan;
    @Autowired
    public QuerySignatures(QuerySignaturePlan querySignaturePlan){
        this.querySignaturePlan=querySignaturePlan;
    }
    @Override
    public QueryPlan createQueryPlan(QueryWrapper qw, Metadata m) {
        return querySignaturePlan;
    }
}
