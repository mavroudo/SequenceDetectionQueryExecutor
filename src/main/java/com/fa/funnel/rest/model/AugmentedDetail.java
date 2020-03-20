package com.fa.funnel.rest.model;

import java.util.Objects;

/**
 * Represents an detail that is augmented by the event name to which it corresponds
 * @author Andreas Kosmatopoulos
 */
public class AugmentedDetail implements Comparable<AugmentedDetail>
{
    /**
     * The event name
     */
    public String evName;

    /**
     * The detail key
     */
    public String key;

    /**
     * The detail value
     */
    public String value;

    /**
     * Constructor
     * @param evName The event name
     * @param key The detail key
     * @param value The detail value
     */
    public AugmentedDetail(String evName, String key, String value)
    {
        this.evName = evName;
        this.key = key;
        this.value = value;
    }

    /**
     * Returns the first field of the AugmentedDetail (i.e. event name)
     * @return the event name
     */
    public String getFirst()
    {
        return evName;
    }

    /**
     * Returns the second field of the AugmentedDetail (i.e. detail key)
     * @return the detail key
     */
    public String getSecond()
    {
        return key;
    }

    /**
     * Returns the third field of the AugmentedDetail (i.e. detail value)
     * @return the detail value
     */
    public String getThird()
    {
        return value;
    }
    
    @Override
    public String toString()
    {
        return "(" + evName + "_" + key + "," + value + ")";
    }

    @Override
    public int compareTo(AugmentedDetail t)
    {
        String thisDetail = evName + "_" + key + "_" + value;
        String otherDetail = t.evName + "_" + t.key + "_" + t.value;
        
        return thisDetail.compareTo(otherDetail);
    }

    @Override
    public int hashCode() 
    {
        int hash = 7;
        hash = 19 * hash + Objects.hashCode(this.evName);
        hash = 19 * hash + Objects.hashCode(this.key);
        hash = 19 * hash + Objects.hashCode(this.value);
        return hash;
    }

    @Override
    public boolean equals(Object obj) 
    {
        if (this == obj) 
        {
            return true;
        }
        if (obj == null) 
        {
            return false;
        }
        if (getClass() != obj.getClass()) 
        {
            return false;
        }
        final AugmentedDetail other = (AugmentedDetail) obj;
        if (!Objects.equals(this.evName, other.evName)) 
        {
            return false;
        }
        if (!Objects.equals(this.key, other.key)) 
        {
            return false;
        }
        return Objects.equals(this.value, other.value);
    }
    
    
}
