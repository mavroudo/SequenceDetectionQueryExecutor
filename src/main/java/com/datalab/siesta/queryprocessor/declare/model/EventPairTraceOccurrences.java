package com.datalab.siesta.queryprocessor.declare.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class EventPairTraceOccurrences {
    private String eventA;
    private String eventB;
    private String traceId;
    private List<Integer> occurrencesA;
    private List<Integer> occurrencesB;
}
