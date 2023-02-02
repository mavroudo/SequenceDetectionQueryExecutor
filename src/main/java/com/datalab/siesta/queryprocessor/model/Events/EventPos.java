package com.datalab.siesta.queryprocessor.model.Events;

public class EventPos extends Event{

    protected int position;

    public EventPos() {
        this.position=-1;
    }

    public EventPos(String name, int pos) {
        super(name);
        this.position=pos;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    @Override
    public String toString() {
        return "EventPos{" +
                "position=" + position +
                ", name='" + name + '\'' +
                '}';
    }
}
