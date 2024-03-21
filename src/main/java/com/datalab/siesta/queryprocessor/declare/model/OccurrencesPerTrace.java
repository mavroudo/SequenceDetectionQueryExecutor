package com.datalab.siesta.queryprocessor.declare.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * This class contains the number of occurrences (for a specific event type) within a trace
 * it is combined with the {@link UniqueTracesPerEventType}
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class OccurrencesPerTrace {
    private long traceId;
    private int occurrences;
}
