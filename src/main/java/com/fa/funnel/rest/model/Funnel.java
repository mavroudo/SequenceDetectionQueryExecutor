package com.fa.funnel.rest.model;

import java.util.ArrayList;
import java.util.List;

/**
 * A Funnel body as provided by the JSON input (i.e. not wrapped by a {@code funnel} tag
 * @author Andreas Kosmatopoulos
 */
public class Funnel
{
    private List<Step> steps;
    private long maxDuration;

    public void setSteps(List<Step> steps)
    {
        this.steps = steps;
    }

    public void setMaxDuration(long maxDuration)
    {
        this.maxDuration = maxDuration;
    }

    public List<Step> getSteps() { return this.steps; }

    public long getMaxDuration() { return this.maxDuration; }

    public Funnel()
    {
        steps = new ArrayList<>();
    }

    @Override
    public String toString()
    {
        StringBuilder json = new StringBuilder("{" +
                "steps: [");
        for(Step step : steps) {
            json.append(step);
            json.append(",");
        }
        json.append("],");
        json.append("max_duration: ");
        json.append(maxDuration);
        json.append("}");
        return json.toString();
    }

    public static class Builder
    {
        private final Funnel funnel = new Funnel();

        public Builder steps(List<Step> steps)
        {
            funnel.steps = steps;
            return this;
        }

        public Builder step(Step step)
        {
            funnel.steps.add(step);
            return this;
        }

        public Funnel build()
        {
            return funnel;
        }
    }
}
