package com.datalab.siesta.queryprocessor.declare.queryWrappers;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class QueryOrderRelationWrapper extends QueryWrapperDeclare{
    private String mode;
    private String constraint;

    public QueryOrderRelationWrapper(double support){
        super(support);
    }

}
