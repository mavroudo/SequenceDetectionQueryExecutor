package com.datalab.siesta.queryprocessor.Signatures;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest

class CassandraConnectionSignatureTest {

    @Autowired
    private CassandraConnectionSignature cassandraConnectionSignature;
    @Test
    void getSignature() {
        cassandraConnectionSignature.getSignature("bpi_2017");
    }
}