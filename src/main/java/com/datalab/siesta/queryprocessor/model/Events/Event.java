package com.datalab.siesta.queryprocessor.model.Events;

public abstract class Event {

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
}
