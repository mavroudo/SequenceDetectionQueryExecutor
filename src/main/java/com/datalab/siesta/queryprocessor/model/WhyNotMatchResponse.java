package com.datalab.siesta.queryprocessor.model;

import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.PossibleOrderOfEvents;


import java.util.ArrayList;
import java.util.List;

public class WhyNotMatchResponse {

    private List<PossibleOrderOfEvents> found;

    private List<List<List<Event>>> foundPatterns;

    private List<PossibleOrderOfEvents> notFound;


    public WhyNotMatchResponse() {
        this.found=new ArrayList<>();
        this.foundPatterns=new ArrayList<>();
        this.notFound=new ArrayList<>();
    }


    public void addFound(PossibleOrderOfEvents p){
        found.add(p);
        foundPatterns.add(new ArrayList<>());
    }
    public void addNotFount(PossibleOrderOfEvents p){notFound.add(p);}
    public void addMatchToLast(List<Event> match){
        foundPatterns.get(foundPatterns.size()-1).add(match);
    }

    public List<PossibleOrderOfEvents> getFound() {
        return found;
    }

    public List<List<List<Event>>> getFoundPatterns() {
        return foundPatterns;
    }

    public List<PossibleOrderOfEvents> getNotFound() {
        return notFound;
    }


}
