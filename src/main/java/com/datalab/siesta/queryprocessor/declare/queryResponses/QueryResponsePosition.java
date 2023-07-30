package com.datalab.siesta.queryprocessor.declare.queryResponses;

import com.datalab.siesta.queryprocessor.declare.model.EventSupport;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import scala.Tuple2;

import java.util.List;
import java.util.stream.Collectors;

public class QueryResponsePosition implements QueryResponse {

    @JsonProperty("first")
    private List<EventSupport> first;

    @JsonProperty("last")
    private List<EventSupport> last;

    public QueryResponsePosition() {
    }

    public List<EventSupport> getFirst() {
        return first;
    }

    public void setFirstTuple(List<Tuple2<String,Double>> recs){
        this.first = recs.stream().map(x->new EventSupport(x._1,x._2))
                .collect(Collectors.toList());
    }

    public void setLastTuple(List<Tuple2<String,Double>> recs){
        this.last = recs.stream().map(x->new EventSupport(x._1,x._2))
                .collect(Collectors.toList());
    }

    public void setFirst(List<EventSupport> first) {
        this.first = first;
    }

    public List<EventSupport> getLast() {
        return last;
    }

    public void setLast(List<EventSupport> last) {
        this.last = last;
    }
}
