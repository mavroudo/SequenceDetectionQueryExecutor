package com.datalab.siesta.queryprocessor.model;

import com.datalab.siesta.queryprocessor.model.Events.EventPair;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class ExtractedPairsForPatternDetection {

    private Set<EventPair> truePairs;

    private Set<EventPair> allPairs;

    public ExtractedPairsForPatternDetection() {
        this.allPairs=new HashSet<>();
        this.truePairs=new HashSet<>();
    }

    public void addTruePairs(Collection<EventPair> pairs){
        this.truePairs.addAll(pairs);
    }

    public void addPairs(Collection<EventPair> pairs){
        this.allPairs.addAll(pairs);
    }

    public void addPair(EventPair pair){
        this.allPairs.add(pair);
    }

    public void addTruePair(EventPair pair){
        this.truePairs.add(pair);
    }

    public Set<EventPair> getTruePairs() {
        return truePairs;
    }

    public Set<EventPair> getAllPairs() {
        return allPairs;
    }
}
