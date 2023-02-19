package com.datalab.siesta.queryprocessor.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class GroupConfigTest {

    @Test
    void setGroups() {
        GroupConfig groupConfig = new GroupConfig("[(1-3),(4)]");
        List<Set<Long>> groups = groupConfig.getGroups();
        Assertions.assertEquals(2,groups.size());
        Assertions.assertEquals(3,groups.get(0).size());
        Assertions.assertEquals(1,groups.get(1).size());

    }
}