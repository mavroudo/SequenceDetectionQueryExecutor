package com.datalab.siesta.queryprocessor.declare.queryWrappers;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class QueryExistenceWrapper extends QueryWrapperDeclare{

    private List<String> modes;

    public QueryExistenceWrapper(double support){
        super(support);
        this.modes = new ArrayList<>();

    }

}
