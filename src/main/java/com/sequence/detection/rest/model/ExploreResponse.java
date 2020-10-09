package com.sequence.detection.rest.model;

import java.util.List;
import java.util.Objects;

/**
 * Represents the output of an {@code explore} endpoint query
 * @author Andreas Kosmatopoulos
 */
public class ExploreResponse
{
    private List<Proposition> propositions;

    public List<Proposition> getPropositions() { return propositions; }

    public void setPropositions(List<Proposition> propositions)
    {
        this.propositions = propositions;
    }

    @Override
    public int hashCode()
    {
        int hash = 17;
        hash = 31 * hash + propositions.hashCode();

        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ExploreResponse other = (ExploreResponse) obj;
        return Objects.equals(this.propositions, other.propositions);
    }

    @Override
    public String toString()
    {
        StringBuilder json = new StringBuilder("{ ");
        json.append(" propositions: [");
        for (Proposition proposition : propositions) {
            json.append(proposition).append(',');
        }
        json.append(" ] }");
        return json.toString();
    }
}
