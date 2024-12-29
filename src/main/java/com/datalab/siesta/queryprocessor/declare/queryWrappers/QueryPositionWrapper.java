package com.datalab.siesta.queryprocessor.declare.queryWrappers;


import lombok.Getter;
import lombok.Setter;

/**
 * Contains the required parameters for the extraction of the position
 * constraints. These
 * are the type: first, last, both and the indexed events and traces in the
 * state.
 * The last two metrics will be used by the QueryPositions.java to define the
 * correct query plan
 * 
 * @see com.datalab.siesta.queryprocessor.declare.queries.QueryPositions
 */
@Getter
@Setter
public class QueryPositionWrapper extends QueryWrapperDeclare {

    protected String mode;

    public QueryPositionWrapper(double support){
        super(support);
        this.mode="both";
    }

}
