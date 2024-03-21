package com.datalab.siesta.queryprocessor.declare.queryResponses;

import com.datalab.siesta.queryprocessor.declare.model.EventN;
import com.datalab.siesta.queryprocessor.declare.model.EventPairSupport;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

public class QueryResponseExistence implements QueryResponse {

    @JsonProperty("existence")
    private List<EventN> existence;

    private List<EventN> absence;
    private List<EventN> exactly;
    @JsonProperty("co-existence")
    private List<EventPairSupport> coExistence;
    @JsonProperty("not co-existence")
    private List<EventPairSupport> notCoExistence;
    @JsonProperty("choice")
    private List<EventPairSupport> choice;
    @JsonProperty("exclusive choice")
    private List<EventPairSupport> exclusiveChoice;
    @JsonProperty("responded existence")
    private List<EventPairSupport> respondedExistence;

    public QueryResponseExistence() {
    }

    public List<EventN> getExistence() {
        return existence;
    }

    public void setExistence(List<EventN> existence) {
        this.existence = existence;
    }

    public List<EventN> getAbsence() {
        return absence;
    }

    public void setAbsence(List<EventN> absence) {
        this.absence = absence;
    }

    public List<EventN> getExactly() {
        return exactly;
    }

    public void setExactly(List<EventN> exactly) {
        this.exactly = exactly;
    }

    public List<EventPairSupport> getCoExistence() {
        return coExistence;
    }

    public void setCoExistence(List<EventPairSupport> coExistence) {
        this.coExistence = coExistence;
    }

    public List<EventPairSupport> getNotCoExistence() {
        return notCoExistence;
    }

    public void setNotCoExistence(List<EventPairSupport> notCoExistence) {
        this.notCoExistence = notCoExistence;
    }

    public List<EventPairSupport> getChoice() {
        return choice;
    }

    public void setChoice(List<EventPairSupport> choice) {
        this.choice = choice;
    }

    public List<EventPairSupport> getExclusiveChoice() {
        return exclusiveChoice;
    }

    public void setExclusiveChoice(List<EventPairSupport> exclusiveChoice) {
        this.exclusiveChoice = exclusiveChoice;
    }

    public List<EventPairSupport> getRespondedExistence() {
        return respondedExistence;
    }

    public void setRespondedExistence(List<EventPairSupport> respondedExistence) {
        this.respondedExistence = respondedExistence;
    }
}
