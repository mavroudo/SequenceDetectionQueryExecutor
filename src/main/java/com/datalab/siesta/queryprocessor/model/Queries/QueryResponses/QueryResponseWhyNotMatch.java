package com.datalab.siesta.queryprocessor.model.Queries.QueryResponses;

import com.datalab.siesta.queryprocessor.model.Occurrences;
import com.datalab.siesta.queryprocessor.model.WhyNotMatch.AlmostMatch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryResponseWhyNotMatch implements QueryResponse{

    List<Occurrences> trueOccurrences;

    Map<Long,String> almostOccurrences;

    public QueryResponseWhyNotMatch() {
    }

    public QueryResponseWhyNotMatch(List<Occurrences> trueOccurrences, List<AlmostMatch> almostMatches) {
        this.trueOccurrences = trueOccurrences;
        almostOccurrences = new HashMap<>(){{
            for(AlmostMatch am : almostMatches)
                put(am.getTrace_id(),am.getRecommendation());
        }};
    }

    public List<Occurrences> getTrueOccurrences() {
        return trueOccurrences;
    }

    public void setTrueOccurrences(List<Occurrences> trueOccurrences) {
        this.trueOccurrences = trueOccurrences;
    }

    public Map<Long, String> getAlmostOccurrences() {
        return almostOccurrences;
    }

    public void setAlmostOccurrences(Map<Long, String> almostOccurrences) {
        this.almostOccurrences = almostOccurrences;
    }

    public void setAlmostOccurrences(List<AlmostMatch> almostMatches){
        almostOccurrences = new HashMap<>(){{
            for(AlmostMatch am : almostMatches)
                put(am.getTrace_id(),am.getRecommendation());
        }};
    }
}
