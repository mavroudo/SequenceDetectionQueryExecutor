package com.datalab.siesta.queryprocessor.model.WhyNotMatch;

import org.codehaus.jackson.annotate.JsonIgnore;

public class WhyNotMatchConfig {

    private int k;
    private String granularityK;
    private int uncertaintyPerEvent;

    private String granularityUncertainty;

    public WhyNotMatchConfig(int k, int uncertaintyPerEvent) {
        this.k = k;
        granularityK="seconds";
        granularityUncertainty="seconds";
        this.uncertaintyPerEvent = uncertaintyPerEvent;
    }

    public WhyNotMatchConfig() {
        this.k=30;
        this.granularityK="seconds";
        this.uncertaintyPerEvent=5;
        this.granularityUncertainty="seconds";
    }

    public WhyNotMatchConfig(int k, String granularityK, int uncertaintyPerEvent, String granularityUncertainty) {
        this.k = k;
        this.granularityK = granularityK;
        this.uncertaintyPerEvent = uncertaintyPerEvent;
        this.granularityUncertainty = granularityUncertainty;
    }


    @JsonIgnore
    public int getStepInSeconds(){
        if(granularityUncertainty.equals("minutes")) return 60;
        else if (granularityUncertainty.equals("hours")) return 60*60;
        else return 1;
    }

    public int getK() {
        if(granularityK.equals("minutes")) return k*60;
        else if (granularityK.equals("hours")) return k*60*60;
        else return k;
    }

    public void setK(int k) {
        this.k = k;
    }

    public String getGranularityK() {
        return granularityK;
    }

    public void setGranularityK(String granularityK) {
        this.granularityK = granularityK;
    }

    public int getUncertaintyPerEvent() {
        if(granularityUncertainty.equals("minutes")) return uncertaintyPerEvent*60;
        else if (granularityUncertainty.equals("hours")) return uncertaintyPerEvent*60*60;
        else return uncertaintyPerEvent;
    }

    public void setUncertaintyPerEvent(int uncertaintyPerEvent) {
        this.uncertaintyPerEvent = uncertaintyPerEvent;
    }

    public String getGranularityUncertainty() {
        return granularityUncertainty;
    }

    public void setGranularityUncertainty(String granularityUncertainty) {
        this.granularityUncertainty = granularityUncertainty;
    }
}
