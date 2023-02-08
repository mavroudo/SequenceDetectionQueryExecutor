package com.datalab.siesta.queryprocessor.model.Queries.Wrapper;

import com.datalab.siesta.queryprocessor.model.Patterns.ComplexPattern;


public class QueryPatternDetectionWrapper extends QueryWrapper {

    private ComplexPattern pattern;

    private boolean whyNotMatchFlag;

    public QueryPatternDetectionWrapper() {
    }

    public ComplexPattern getPattern() {
        return pattern;
    }

    public void setPattern(ComplexPattern pattern) {
        this.pattern = pattern;
    }

    public boolean isWhyNotMatchFlag() {
        return whyNotMatchFlag;
    }

    public void setWhyNotMatchFlag(boolean whyNotMatchFlag) {
        this.whyNotMatchFlag = whyNotMatchFlag;
    }
}
