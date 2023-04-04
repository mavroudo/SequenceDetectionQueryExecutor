package com.datalab.siesta.queryprocessor.model.Queries.Wrapper;

import com.datalab.siesta.queryprocessor.model.GroupConfig;
import com.datalab.siesta.queryprocessor.model.Patterns.ComplexPattern;
import com.datalab.siesta.queryprocessor.model.WhyNotMatch.WhyNotMatchConfig;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.Timestamp;


public class QueryPatternDetectionWrapper extends QueryWrapper {

    private ComplexPattern pattern;

    private boolean whyNotMatchFlag;

    private boolean hasGroups;

    private Timestamp from;

    private Timestamp till;

    @JsonProperty("wnm-config")
    private WhyNotMatchConfig whyNotMatchConfig;

    @JsonProperty("groups-config")
    private GroupConfig groupConfig;

    private boolean returnAll;

    public QueryPatternDetectionWrapper() {
        this.returnAll=false;
        this.whyNotMatchFlag=false;
        this.hasGroups=false;
        this.from=null;
        this.till=null;
        this.groupConfig=new GroupConfig();
        this.whyNotMatchConfig = new WhyNotMatchConfig();
    }

    public ComplexPattern getPattern() {
        return pattern;
    }

    public void setPattern(ComplexPattern pattern) {
        this.pattern = pattern;
    }

    public boolean isWhyNotMatchFlag() {
        return whyNotMatchFlag;
    }

    public void setWhyNotMatchFlag(boolean whyNotMatchFlag) {
        this.whyNotMatchFlag = whyNotMatchFlag;
    }

    public boolean isReturnAll() {
        return returnAll;
    }

    public void setReturnAll(boolean returnAll) {
        this.returnAll = returnAll;
    }

    @JsonIgnore
    public int getK() {
        return whyNotMatchConfig.getK();
    }

    @JsonIgnore
    public int getUncertainty(){
        return whyNotMatchConfig.getUncertaintyPerEvent();
    }

    @JsonIgnore
    public int getStepInSeconds(){
        return whyNotMatchConfig.getStepInSeconds();
    }


    public WhyNotMatchConfig getWhyNotMatchConfig() {
        return whyNotMatchConfig;
    }

    public void setWhyNotMatchConfig(WhyNotMatchConfig whyNotMatchConfig) {
        this.whyNotMatchConfig = whyNotMatchConfig;
    }

    public boolean isHasGroups() {
        return hasGroups;
    }

    public void setHasGroups(boolean hasGroups) {
        this.hasGroups = hasGroups;
    }

    public GroupConfig getGroupConfig() {
        return groupConfig;
    }

    public void setGroupConfig(GroupConfig groupConfig) {
        this.groupConfig = groupConfig;
    }

    public Timestamp getFrom() {
        return from;
    }

    public void setFrom(Timestamp from) {
        this.from = from;
    }

    public Timestamp getTill() {
        return till;
    }

    public void setTill(Timestamp till) {
        this.till = till;
    }
}
