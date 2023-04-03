package edu.umass.cs.sase.query;

import edu.umass.cs.sase.stream.Event;
import net.sourceforge.jeval.EvaluationException;

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

    private void initialize() {
        if (this.stateType.equalsIgnoreCase("or") || this.stateType.equalsIgnoreCase("negative")) {
            this.isKleeneClosure = false;
            this.isNegation = false;
            this.edges = new Edge[1];
            this.edges[0] = new Edge(0);
        } else if (this.stateType.equalsIgnoreCase("kleeneClosure*")) {
            this.isKleeneClosure = true;
            this.isNegation = false;
            this.edges = new Edge[3];
            for (int i = 0; i < 3; i++) {
                this.edges[i] = new Edge(i);
            }
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

    @Override
    public boolean canStartWithEvent(Event e) throws EvaluationException{
        long c = this.eventList.stream().filter(x->x.equalsIgnoreCase(e.getEventType())).count();
        if(c==0) return false;
        if(this.edges[0].evaluatePredicate(e,e)){
            return true;
        }
        return false;

    }

}
