package com.fa.funnel.rest.model;

/**
 * A Step Detail as provided by the JSON input
 * @author Andreas Kosmatopoulos
 */
public class Detail
{
    private String key;
    private String operator;
    private String value;


    public void setKey(String key)
    {
        this.key = key;
    }

    public void setOperator(String operator)
    {
        this.operator = operator;
    }

    public void setValue(String value)
    {
        this.value = value;
    }

    public String getKey() { return this.key; }

    public String getOperator() { return this.operator; }

    public String getValue() { return value; }

    @Override
    public String toString()
    {
        StringBuilder json = new StringBuilder("{ ");
        json.append(" key: ").append(key);
        json.append(" operator: ").append(operator);
        json.append(" value: ").append(value);
        json.append("}");
        return json.toString();
    }


    public static class Builder
    {
        private final Detail detail = new Detail();

        public Builder key(String key)
        {
            detail.key = key;
            return this;
        }

        public Builder operator(String operator)
        {
            detail.operator = operator;
            return this;
        }

        public Builder value(String value)
        {
            detail.value = value;
            return this;
        }

        public Detail build()
        {
            return detail;
        }
    }
}
