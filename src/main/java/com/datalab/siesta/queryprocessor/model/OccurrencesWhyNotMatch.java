package com.datalab.siesta.queryprocessor.model;

import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.List;
import java.util.stream.Collectors;

public class OccurrencesWhyNotMatch extends  Occurrences{

    private PossibleOrderOfEvents possibleOrderOfEvents;

    public OccurrencesWhyNotMatch() {
    }

    public OccurrencesWhyNotMatch(long traceID, List<Occurrence> occurrences, PossibleOrderOfEvents possibleOrderOfEvents) {
        super(traceID, occurrences);
        this.possibleOrderOfEvents = possibleOrderOfEvents;
    }

    public PossibleOrderOfEvents getPossiblePattern() {
        return possibleOrderOfEvents;
    }

    public void setPossiblePattern(PossibleOrderOfEvents possibleOrderOfEvents) {
        this.possibleOrderOfEvents = possibleOrderOfEvents;
    }

    @JsonIgnore
    public void evaluateConstraints(){
        List<Occurrence> meetConstraints = this.occurrences.stream().filter(x-> possibleOrderOfEvents.evaluatePatternConstraints(x))
                .collect(Collectors.toList());
        this.setOccurrences(meetConstraints);
    }
}
