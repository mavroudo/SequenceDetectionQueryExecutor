package com.sequence.detection.rest.model;

import java.util.List;
import java.util.Objects;

/**
 * Represents the output of an {@code quick_stats} endpoint query
 * @author Andreas Kosmatopoulos
 */
public class QuickStatsResponse
{
    private List<Completion> completions;

    public List<Completion> getCompletions() { return completions; }

    public void setCompletions(List<Completion> completions)
    {
        this.completions = completions;
    }

    @Override
    public int hashCode()
    {
        int hash = 17;
        hash = 31 * hash + completions.hashCode();

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
        final QuickStatsResponse other = (QuickStatsResponse) obj;
        return Objects.equals(this.completions, other.completions);
    }

    @Override
    public String toString()
    {
        StringBuilder json = new StringBuilder("{ ");
        json.append(" completions: [");
        for (Completion completion : completions) {
            json.append(completion).append(',');
        }
        json.append(" ] }");
        return json.toString();
    }
}
