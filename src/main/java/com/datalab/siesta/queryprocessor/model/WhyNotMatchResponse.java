package com.datalab.siesta.queryprocessor.model;

import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.PossiblePattern;


import java.util.ArrayList;
import java.util.List;

public class WhyNotMatchResponse {

    private List<PossiblePattern> found;

    private List<List<List<Event>>> foundPatterns;

    private List<PossiblePattern> notFound;


    public WhyNotMatchResponse() {
        this.found=new ArrayList<>();
        this.foundPatterns=new ArrayList<>();
        this.notFound=new ArrayList<>();
    }


    public void addFound(PossiblePattern p){
        found.add(p);
        foundPatterns.add(new ArrayList<>());
    }
    public void addNotFount(PossiblePattern p){notFound.add(p);}
    public void addMatchToLast(List<Event> match){
        foundPatterns.get(foundPatterns.size()-1).add(match);
    }

    public List<PossiblePattern> getFound() {
        return found;
    }

    public List<List<List<Event>>> getFoundPatterns() {
        return foundPatterns;
    }

    public List<PossiblePattern> getNotFound() {
        return notFound;
    }


}
