package com.sequence.detection.rest.setcontainment;

import com.sequence.detection.rest.model.Event;
import com.sequence.detection.rest.model.QueryPair;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class LCJoinTest {

    @Test
    public void testBinarySearch()
    {
        ArrayList<Long> e1 = new ArrayList<>();
        e1.add(1L);
        e1.add(2L);
        e1.add(3L);
        e1.add(7L);
        ArrayList<Long> e2 = new ArrayList<>();
        e2.add(3L);
        e2.add(4L);
        e2.add(5L);
        e2.add(6L);
        e2.add(7L);
        ArrayList<Long> e3 = new ArrayList<>();
        e3.add(1L);
        e3.add(2L);
        e3.add(3L);
        e3.add(5L);
        e3.add(6L);
        e3.add(7L);
        ArrayList<Long> e4 = new ArrayList<>();
        e4.add(1L);
        e4.add(3L);
        e4.add(4L);
        e4.add(5L);
        e4.add(6L);
        List<List<Long>> invertedLists = new ArrayList<>();
        invertedLists.add(e1);
        invertedLists.add(e2);
        invertedLists.add(e3);
        invertedLists.add(e4);
        List<Long> ids = LCJoin.crossCuttingBasedIntersection(invertedLists);
        Assert.assertTrue(ids.contains(3L));
        Assert.assertTrue(ids.size()==1);


    }

}