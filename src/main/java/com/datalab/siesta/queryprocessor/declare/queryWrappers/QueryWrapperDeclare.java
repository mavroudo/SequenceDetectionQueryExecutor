package com.datalab.siesta.queryprocessor.declare.queryWrappers;

import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class QueryWrapperDeclare extends QueryWrapper {


    protected double support;
    protected boolean stateAvailable;
    protected boolean isStateUpToDate;
    protected int indexedEvents;
    protected int indexedTraces;
    protected boolean enforceNormalMining;

    public QueryWrapperDeclare(double support) {
        this.support = support;
        this.indexedEvents = 0;
        this.indexedTraces = 0;
        this.stateAvailable = false;
        this.isStateUpToDate = false;
        this.enforceNormalMining=false;
    }


}
