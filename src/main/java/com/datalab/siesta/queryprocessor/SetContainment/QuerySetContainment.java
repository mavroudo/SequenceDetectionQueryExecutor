package com.datalab.siesta.queryprocessor.SetContainment;

import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.Query;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class QuerySetContainment implements Query {

    private QuerySetContainmentPlan querySetContainmentPlan;

    @Autowired
    public QuerySetContainment(QuerySetContainmentPlan querySetContainmentPlan){
        this.querySetContainmentPlan=querySetContainmentPlan;
    }

    @Override
    public QueryPlan createQueryPlan(QueryWrapper qw, Metadata m) {
        return querySetContainmentPlan;
    }
}
