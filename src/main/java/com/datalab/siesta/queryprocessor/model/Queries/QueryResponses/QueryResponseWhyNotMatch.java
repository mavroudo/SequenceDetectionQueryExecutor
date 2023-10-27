package com.datalab.siesta.queryprocessor.model.Queries.QueryResponses;

import com.datalab.siesta.queryprocessor.model.Occurrences;
import com.datalab.siesta.queryprocessor.model.WhyNotMatch.AlmostMatch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryResponseWhyNotMatch extends  QueryResponsePatternDetection{

    Map<Long,String> almostOccurrences;

    public QueryResponseWhyNotMatch() {
    }

    public QueryResponseWhyNotMatch(List<Occurrences> trueOccurrences, List<AlmostMatch> almostMatches) {
        this.occurrences = trueOccurrences;
        almostOccurrences = new HashMap<>(){{
            for(AlmostMatch am : almostMatches)
                put(am.getTrace_id(),am.getRecommendation());
        }};
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
