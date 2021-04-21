package com.sequence.detection.rest.setcontainment;

import com.sequence.detection.rest.model.Event;
import com.sequence.detection.rest.model.Sequence;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;

import static org.junit.Assert.*;

public class VerifyPatternTest {

    @Test
    public void testSearch() {
        ArrayList<String> events = new ArrayList<>();
        events.add("a");
        events.add("b");
        events.add("c");
        events.add("a");
        events.add("b");
        events.add("b");

        ArrayList<Event> s1 = new ArrayList<>();
        s1.add(new Event("a"));
        s1.add(new Event("b"));
        s1.add(new Event("c"));

        ArrayList<Event> s2 = new ArrayList<>();
        s2.add((new Event("a")));
        s2.add(new Event("b"));
        s2.add(new Event("a"));

        ArrayList<Event> s3 = new ArrayList<>();
        s3.add(new Event("a"));
        s3.add(new Event("a"));
        s3.add((new Event("c")));

        Event[] eventsArray = new Event[s1.size()];
        eventsArray = s1.toArray(eventsArray);
        Sequence s1seq = new Sequence(eventsArray);


        Assert.assertTrue(VerifyPattern.verifyPattern(s1seq, events, "strict"));
        Assert.assertTrue(VerifyPattern.verifyPattern(s1seq, events, "skiptillnextmatch"));

        eventsArray = new Event[s2.size()];
        eventsArray = s2.toArray(eventsArray);
        Sequence s2seq = new Sequence(eventsArray);

        Assert.assertFalse(VerifyPattern.verifyPattern(s2seq, events, "strict"));
        Assert.assertTrue(VerifyPattern.verifyPattern(s2seq, events, "skiptillnextmatch"));

        eventsArray = new Event[s3.size()];
        eventsArray = s3.toArray(eventsArray);
        Sequence s3seq = new Sequence(eventsArray);
        Assert.assertFalse(VerifyPattern.verifyPattern(s3seq, events, "strict"));
        Assert.assertFalse(VerifyPattern.verifyPattern(s3seq, events, "skiptillnextmatch"));


    }

}