package com.datalab.siesta.queryprocessor.Signatures;

import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.Query;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import org.springframework.stereotype.Service;

@Service
public class QuerySignatures implements Query {
    @Override
    public QueryPlan createQueryPlan(QueryWrapper qw, Metadata m) {
        return null;
    }
}
