package com.datalab.siesta.queryprocessor.model.Events;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.util.Assert;


import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;



class EventBothTest {

    @Test
    public void testSerialization() throws IOException {
        ObjectMapper om = new ObjectMapper();
        EventBoth eb1 = new EventBoth("A", Timestamp.valueOf(LocalDateTime.now()),5);
        String s1 = om.writeValueAsString(eb1);
        Assertions.assertTrue(s1.contains("name"));
        Assertions.assertTrue(s1.contains("timestamp"));
        Assertions.assertTrue(s1.contains("position"));

        EventBoth eb2 = new EventBoth("A", Timestamp.valueOf(LocalDateTime.now()),-1);
        String s2 = om.writeValueAsString(eb2);
        Assertions.assertTrue(s2.contains("name"));
        Assertions.assertTrue(s2.contains("timestamp"));
        Assertions.assertFalse(s2.contains("position"));

        EventBoth eb3 = new EventBoth("A", null,5);
        String s3 = om.writeValueAsString(eb3);
        Assertions.assertTrue(s3.contains("name"));
        Assertions.assertFalse(s3.contains("timestamp"));
        Assertions.assertTrue(s3.contains("position"));

    }

}