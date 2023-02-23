package com.datalab.siesta.queryprocessor.Signatures;

import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;

public class QuerySignaturePlan implements QueryPlan {

    private CassandraConnectionSignature cassandraConnectionSignature;

    @Autowired
    public QuerySignaturePlan(CassandraConnectionSignature cassandraConnectionSignature){
        this.cassandraConnectionSignature=cassandraConnectionSignature;

    }

    @Override
    public QueryResponse execute(QueryWrapper qw) {
        //TODO: create signature from metadata

        //TODO: create query from Cassandra

        //TODO: get traces - records

        //TODO: pass it through sase

        //TODO: create response
        return null;
    }

    @Override
    public void setMetadata(Metadata metadata) {
        //no need for metadata
    }
}
