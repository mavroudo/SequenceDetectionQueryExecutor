package com.datalab.siesta.queryprocessor.model.Queries.QueryResponses;

import com.datalab.siesta.queryprocessor.model.Occurrences;
import com.datalab.siesta.queryprocessor.model.PossibleOrderOfEvents;

import java.util.List;

public class QueryResponseWhyNotMatch implements QueryResponse{

    List<Occurrences> trueOccurrences;

    List<PossibleOrderOfEvents> almostOccurrences;

    public QueryResponseWhyNotMatch() {
    }

    public QueryResponseWhyNotMatch(List<Occurrences> trueOccurrences, List<PossibleOrderOfEvents> almostOccurrences) {
        this.trueOccurrences = trueOccurrences;
        this.almostOccurrences = almostOccurrences;
    }

    public List<Occurrences> getTrueOccurrences() {
        return trueOccurrences;
    }

    public void setTrueOccurrences(List<Occurrences> trueOccurrences) {
        this.trueOccurrences = trueOccurrences;
    }

    public List<PossibleOrderOfEvents> getAlmostOccurrences() {
        return almostOccurrences;
    }

    public void setAlmostOccurrences(List<PossibleOrderOfEvents> almostOccurrences) {
        this.almostOccurrences = almostOccurrences;
    }
}
