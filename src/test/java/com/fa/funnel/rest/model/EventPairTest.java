package com.fa.funnel.rest.model;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class EventPairTest 
{
    @Test
    public void equals_EqualityCase_Self() 
    {
        EventPair ep1 = new EventPair("a", "b");
        assertTrue(ep1.equals(ep1));
    }
    
    @Test
    public void equals_EqualityCase_Other()
    {
        EventPair ep1 = new EventPair("a", "b");
        EventPair ep2 = new EventPair("a", "b");
        assertTrue(ep1.equals(ep2));
        assertTrue(ep2.equals(ep1));
    }
    
    @Test
    public void equals_InequalityCase_First() 
    {
        EventPair ep1 = new EventPair("a", "b");
        EventPair ep2 = new EventPair("c", "b");
        assertFalse(ep1.equals(ep2));
        assertFalse(ep2.equals(ep1));
    }
    
    @Test
    public void equals_InequalityCase_Second() 
    {
        EventPair ep1 = new EventPair("a", "b");
        EventPair ep2 = new EventPair("a", "c");
        assertFalse(ep1.equals(ep2));
        assertFalse(ep2.equals(ep1));
    }
    
    @Test
    public void equals_InequalityCase_Both() 
    {
        EventPair ep1 = new EventPair("a", "b");
        EventPair ep2 = new EventPair("c", "d");
        assertFalse(ep1.equals(ep2));
        assertFalse(ep2.equals(ep1));
    }
}
