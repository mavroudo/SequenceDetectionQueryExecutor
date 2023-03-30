package edu.umass.cs.sase.query;

import edu.umass.cs.sase.stream.Event;

import java.util.ArrayList;
import java.util.List;

public class AdditionalState extends State{

    private List<String> eventList;


    public AdditionalState(int order, String tag, List<String> eventTypes, String stateType) {
        super(order, tag, eventTypes.get(0), stateType); //this will set the values
        this.eventList = new ArrayList<>();
        this.eventList.addAll(eventTypes);
        initialize();
    }

    public AdditionalState(int order, String tag, String eventType, String stateType) {
        super(order, tag, eventType, stateType); //this will set the values
        this.eventList = new ArrayList<>();
        this.eventList.add(eventType);
        initialize();
    }

    private void initialize(){
        if (this.stateType.equalsIgnoreCase("or") || this.stateType.equalsIgnoreCase("negative")) {
            this.isKleeneClosure = false;
            this.isNegation = false;
            this.edges = new Edge[1];
            this.edges[0] = new Edge(0);
        }
    }

    @Override
    public boolean checkEventType(Event e) {
        for(String eventType:this.eventList){
            if(eventType.equalsIgnoreCase(e.getEventType())){
                return true;
            }
        }
        return false;
    }
}
