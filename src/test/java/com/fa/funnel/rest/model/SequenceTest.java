package com.fa.funnel.rest.model;

import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class SequenceTest 
{
    protected static final String DELAB_DELIMITER = "¦delab¦";
    
    @Test
    public void testGetSecondLastEventUnderscored_EmptySequence()
    {
        Sequence empty_sequence = new Sequence();
        String secondlast = empty_sequence.getSecondLastEventDelimited(DELAB_DELIMITER);
        assertTrue(secondlast.equals(""));
    }
    
    @Test
    public void testGetSecondLastEventUnderscored_OneSizedSequence() 
    {
        Sequence onesized_sequence = new Sequence(new Event("singleEvent"));
        String secondlast = onesized_sequence.getSecondLastEventDelimited(DELAB_DELIMITER);
        assertTrue(secondlast.equals(""));
    }
    
    @Test
    public void testGetSecondLastEventUnderscored_LargerOrEqualThanTwoSequence() 
    {
        Sequence normal_sequence = new Sequence(new Event("A"), new Event("B"));
        String secondlast = normal_sequence.getSecondLastEventDelimited(DELAB_DELIMITER);
        assertTrue(secondlast.equals("A"+DELAB_DELIMITER));
        
        normal_sequence = new Sequence(new Event("A"), new Event("B"), new Event("C"));
        secondlast = normal_sequence.getSecondLastEventDelimited(DELAB_DELIMITER);
        assertTrue(secondlast.equals("B"+DELAB_DELIMITER));
        
        normal_sequence = new Sequence(new Event("E"), new Event("A"), new Event("D"), new Event("F"));
        secondlast = normal_sequence.getSecondLastEventDelimited(DELAB_DELIMITER);
        assertTrue(secondlast.equals("D"+DELAB_DELIMITER));
        
        normal_sequence = new Sequence(new Event("E"), new Event("A"), new Event("D"), new Event("F"), new Event("B"));
        secondlast = normal_sequence.getSecondLastEventDelimited(DELAB_DELIMITER);
        assertTrue(secondlast.equals("F"+DELAB_DELIMITER));
        
        normal_sequence = new Sequence(new Event("E"), new Event("A"), new Event("D"), new Event(""), new Event("B"));
        secondlast = normal_sequence.getSecondLastEventDelimited(DELAB_DELIMITER);
        assertTrue(secondlast.equals(DELAB_DELIMITER));
    }
}
