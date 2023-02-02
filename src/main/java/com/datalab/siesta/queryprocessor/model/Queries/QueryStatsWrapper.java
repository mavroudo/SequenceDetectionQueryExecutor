package com.datalab.siesta.queryprocessor.model.Queries;

import com.datalab.siesta.queryprocessor.model.Patterns.SimplePattern;

public class QueryStatsWrapper extends QueryWrapper{

    private SimplePattern pattern;

    public QueryStatsWrapper() {
    }

    public SimplePattern getPattern() {
        return pattern;
    }

    public void setPattern(SimplePattern pattern) {
        this.pattern = pattern;
    }

    @Override
    public String toString() {
        return "QueryStatsWrapper{" +
                "pattern=" + pattern +
                ", log_name='" + log_name + '\'' +
                '}';
    }
}
