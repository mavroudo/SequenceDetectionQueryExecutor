package com.datalab.siesta.queryprocessor.model;

import com.datalab.siesta.queryprocessor.model.Events.EventPair;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Contains the extracted et-pairs from the query pattern. There are two set of et-pairs: (a) true-pairs, that are
 * required to appear in a trace in order to be a valid option and (b) all-pairs, that contains all the pairs that will
 * be retrieved from the database. The (b) set is only used to make sure that all the required information is available
 * during validation.
 */
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

    public void setTruePairs(Set<EventPair> pairs){
        this.truePairs=pairs;
    }

    public Set<EventPair> getTruePairs() {
        return truePairs;
    }

    public Set<EventPair> getAllPairs() {
        return allPairs;
    }
}
