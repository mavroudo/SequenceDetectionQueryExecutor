package com.datalab.siesta.queryprocessor.model.Queries.Wrapper;

import com.datalab.siesta.queryprocessor.model.Patterns.ComplexPattern;


public class QueryPatternDetectionWrapper extends QueryWrapper {

    private ComplexPattern pattern;

    private boolean whyNotMatchFlag;

    private boolean returnAll;

    public QueryPatternDetectionWrapper() {
        this.returnAll=false;
        this.whyNotMatchFlag=false;
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

    public boolean isReturnAll() {
        return returnAll;
    }

    public void setReturnAll(boolean returnAll) {
        this.returnAll = returnAll;
    }
}
