package com.fa.funnel.rest.model;

/**
 * A Funnel Wrapper as provided by the JSON input (i.e. the {@code funnel} tag that wraps a funnel body)
 * @author Andreas Kosmatopoulos
 */
public class FunnelWrapper
{
    private Funnel funnel;

    public void setFunnel(Funnel funnel)
    {
        this.funnel = funnel;
    }

    public Funnel getFunnel() { return this.funnel; }

    @Override
    public String toString()
    {
        return funnel.toString();
    }

    public static class Builder
    {
        private final FunnelWrapper wrapper = new FunnelWrapper();

        public Builder funnel(Funnel funnel)
        {
            wrapper.funnel = funnel;
            return this;
        }

        public FunnelWrapper build()
        {
            return wrapper;
        }
    }
}
