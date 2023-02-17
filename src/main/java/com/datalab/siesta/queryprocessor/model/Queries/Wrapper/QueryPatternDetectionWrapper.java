package com.datalab.siesta.queryprocessor.model.Queries.Wrapper;

import com.datalab.siesta.queryprocessor.model.Patterns.ComplexPattern;


public class QueryPatternDetectionWrapper extends QueryWrapper {

    private ComplexPattern pattern;

    private boolean whyNotMatchFlag;

    private int k;

    private int uncertaintyPerEvent;

    private boolean returnAll;

    public QueryPatternDetectionWrapper() {
        this.returnAll=false;
        this.whyNotMatchFlag=false;
        k=30; // set by default to 30 seconds
        uncertaintyPerEvent=10; // set by default to 10 seconds;
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

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }

    public int getUncertaintyPerEvent() {
        return uncertaintyPerEvent;
    }

    public void setUncertaintyPerEvent(int uncertaintyPerEvent) {
        this.uncertaintyPerEvent = uncertaintyPerEvent;
    }
}
