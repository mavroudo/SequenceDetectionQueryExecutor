package com.datalab.siesta.queryprocessor.model.Events;

import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.MappingJacksonViews;
import com.fasterxml.jackson.annotation.JsonView;

import java.util.Objects;

public class EventSymbol extends EventPos{

    @JsonView(MappingJacksonViews.EventAllInfo.class)
    protected String symbol;

    public EventSymbol() {
        super();
        this.symbol="";

    }

    public EventSymbol(String symbol) {
        this.symbol = symbol;
    }

    public EventSymbol(String name, int pos, String symbol) {
        super(name, pos);
        this.symbol = symbol;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    @Override
    public int compareTo(Object o) {
        return super.compareTo(o);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        EventSymbol that = (EventSymbol) o;
        return Objects.equals(symbol, that.symbol);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), symbol);
    }
}
