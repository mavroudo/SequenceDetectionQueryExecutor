package com.datalab.siesta.queryprocessor.model.Queries.Wrapper;

import com.datalab.siesta.queryprocessor.model.Patterns.ComplexPattern;


public class QueryPatternDetectionWrapper extends QueryWrapper {

    private ComplexPattern pattern;

    public QueryPatternDetectionWrapper() {
    }

    public ComplexPattern getPattern() {
        return pattern;
    }

    public void setPattern(ComplexPattern pattern) {
        this.pattern = pattern;
    }
}
