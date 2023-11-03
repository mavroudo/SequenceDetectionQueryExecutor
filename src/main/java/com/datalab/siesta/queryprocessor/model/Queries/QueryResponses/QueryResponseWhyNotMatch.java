package com.datalab.siesta.queryprocessor.model.Queries.QueryResponses;

import com.datalab.siesta.queryprocessor.model.Occurrences;
import com.datalab.siesta.queryprocessor.model.WhyNotMatch.AlmostMatch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 *  The response for the pattern detection query when the explainability is set.
 *  It extends the fields from the QueryResponsePatternDetection class by adding one more list with all the
 *  matches that were found after modifying the events' timestamps
 */
public class QueryResponseWhyNotMatch extends  QueryResponsePatternDetection{

    /**
     * A map of the trace id and the a string that describes what changes are required to made in the events' timestamps
     * in order to be a valid match for the query pattern
     */
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
