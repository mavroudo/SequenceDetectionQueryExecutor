package com.datalab.siesta.queryprocessor.model.Events;

import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.MappingJacksonViews;
import com.fasterxml.jackson.annotation.JsonView;

import java.io.Serializable;
import java.util.Objects;

public class Event implements Serializable {


    protected String name;

    public Event() {
        this.name="";
    }

    public Event(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Event{" +
                "name='" + name + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return name.equals(event.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
