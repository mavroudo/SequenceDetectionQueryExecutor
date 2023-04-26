package com.datalab.siesta.queryprocessor.model.Queries.Wrapper;

import com.datalab.siesta.queryprocessor.model.Patterns.SimplePattern;

public class QueryExploreWrapper extends QueryWrapper {

    private SimplePattern pattern;
    private String mode;
    private int k;

    public QueryExploreWrapper() {
    }

    public QueryExploreWrapper(SimplePattern simplePattern) {
        k = 1;
        mode = "fast";
        this.pattern = simplePattern;
    }


    public SimplePattern getPattern() {
        return pattern;
    }

    public void setPattern(SimplePattern pattern) {
        this.pattern = pattern;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }
}
