package com.datalab.siesta.queryprocessor.model.DBModel;

import com.clearspring.analytics.util.Lists;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A grouping of the IndexPairs based on the et-types
 * @see com.datalab.siesta.queryprocessor.model.DBModel.IndexPair
 */
public class IndexRecords {

    private Map<EventTypes, List<IndexPair>> records;

    public IndexRecords(List<Tuple2<Tuple2<String, String>, Iterable<IndexPair>>> results) {
        records = new HashMap<>();
        results.forEach(x -> {
            EventTypes et = new EventTypes(x._1._1, x._1._2);
            records.put(et, Lists.newArrayList(x._2));
        });
    }

    public Map<EventTypes, List<IndexPair>> getRecords() {
        return records;
    }

    public void setRecords(Map<EventTypes, List<IndexPair>> records) {
        this.records = records;
    }
}
