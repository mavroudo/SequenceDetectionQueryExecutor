package com.datalab.siesta.queryprocessor.model.Events;

import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.MappingJacksonViews;
import com.fasterxml.jackson.annotation.JsonView;

public class EventSymbol extends EventPos{

    @JsonView(MappingJacksonViews.EventAllInfo.class)
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


}
