package com.datalab.siesta.queryprocessor.SaseConnection;

import edu.umass.cs.sase.query.NFA;
import org.junit.jupiter.api.Test;

public class EvaluateSase {

    @Test
    void testNegation(){
        NFA nfa = new NFA("skip-till-next-match");
        nfa.parseFastQueryLineStartWithPattern("PATTERN seq(!A a,B b)");
    }
}
