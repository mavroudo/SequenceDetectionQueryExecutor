package com.fa.funnel.rest.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * A Step as provided by the JSON input
 * @author Andreas Kosmatopoulos
 */
public class Step
{
    @JsonProperty("match_name")
    private List<Name> names;

    @JsonProperty("match_details")
    private List<Detail> details;

    public void setMatchName(List<Name> names)
    {
        this.names = names;
    }

    public void setMatchDetails(List<Detail> details)
    {
        this.details = details;
    }

    public List<Name> getMatchName() { return names; }

    public List<Detail> getMatchDetails() { return details; }


    public Step()
    {
        this.names = new ArrayList<>();
        this.details = new ArrayList<>();
    }

    @Override
    public String toString()
    {
        StringBuilder json = new StringBuilder("{ match_name: [");
        for(Name name : names) {
            json.append(name);
            json.append(",");
        }
        json.append(" ], ");
        json.append(" match_details: [");
        for(Detail detail : details) {
            json.append(detail);
            json.append(",");
        }
        json.append(" ] }");

        return json.toString();
    }

    public static class Builder
    {
        private final Step step = new Step();

        public Builder matchNames(List<Name> names)
        {
            step.names = names;
            return this;
        }

        public Builder matchName(Name name)
        {
            step.names.add(name);
            return this;
        }

        public Builder matchDetails(List<Detail> details)
        {
            step.details = details;
            return this;
        }

        public Builder matchDetail(Detail detail)
        {
            step.details.add(detail);
            return this;
        }

        public Step build()
        {
            return step;
        }
    }
}
