package com.datalab.siesta.queryprocessor.model.Events;

public class EventSymbol extends EventPos{

    protected String symbol;

    public EventSymbol() {
        super();
        this.symbol="";

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
    public String toString() {
        return "EventSymbol{" +
                "symbol=" + symbol +
                ", position=" + position +
                ", name='" + name + '\'' +
                '}';
    }
}
