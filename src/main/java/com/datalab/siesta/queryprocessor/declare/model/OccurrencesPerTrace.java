package com.datalab.siesta.queryprocessor.declare.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

/**
 * This class contains the number of occurrences (for a specific event type) within a trace
 * it is combined with the {@link UniqueTracesPerEventType}
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class OccurrencesPerTrace implements Serializable {
    private String traceId;
    private int occurrences;
}
