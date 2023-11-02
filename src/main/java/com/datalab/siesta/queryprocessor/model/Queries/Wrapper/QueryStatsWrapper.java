package com.datalab.siesta.queryprocessor.model.Queries.Wrapper;

import com.datalab.siesta.queryprocessor.model.Patterns.SimplePattern;

/**
 * Stats Query: should have a query pattern (sequence of events) and the name of the log database (which is available
 * in the superclass)
 */
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
