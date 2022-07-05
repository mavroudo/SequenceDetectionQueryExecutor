package com.sequence.detection.rest.util;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class ParseGroupsTest {

    @Test
    public void testParsing(){
        List<Set<Integer>> response = ParseGroups.parse("[(1,2,3-10),(12,15),(20-28)]");
        assertEquals(response.size(),3);
        assertNull(ParseGroups.parse("[(1,ts,3-10,(12,15),(20-28)"));
        assertEquals(ParseGroups.parse("").size(),0);

    }
}
