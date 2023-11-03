package com.datalab.siesta.queryprocessor.model.Queries.Wrapper;

import com.datalab.siesta.queryprocessor.model.Patterns.SimplePattern;

/**
 * Exploration query: except for the log database and the pattern, this query has two more fields: (1) the mode
 * which takes values accurate,fast and hybrid (default = fast) and (2) in case of hybrid the user can define the parameter
 * k (top-k propositions discovered by the fast approach will be executed using the accurate approach)
 */
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
