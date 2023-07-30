package com.datalab.siesta.queryprocessor.declare.queryPlans;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class QueryPositions {

    private QueryPlanFirst queryPlanFirst;
    private QueryPlanLast queryPlanLast;
    private QueryPlanBoth queryPlanBoth;

    @Autowired
    public QueryPositions(QueryPlanFirst queryPlanFirst, QueryPlanLast queryPlanLast, QueryPlanBoth queryPlanBoth) {
        this.queryPlanFirst = queryPlanFirst;
        this.queryPlanLast = queryPlanLast;
        this.queryPlanBoth = queryPlanBoth;
    }

    public QueryPlanPosition getQueryPlan(String position, Metadata metadata){
        if(position.equals("first")){
            queryPlanFirst.setMetadata(metadata);
            return queryPlanFirst;
        }else if(position.equals("last")){
            queryPlanLast.setMetadata(metadata);
            return queryPlanLast;
        }else{
            queryPlanBoth.setMetadata(metadata);
            return queryPlanBoth;
        }

    }
}
