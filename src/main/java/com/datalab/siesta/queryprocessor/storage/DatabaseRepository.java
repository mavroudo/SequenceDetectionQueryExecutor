package com.datalab.siesta.queryprocessor.storage;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.EventPair;
import com.datalab.siesta.queryprocessor.model.Metadata;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface DatabaseRepository {

    Metadata getMetadata(String logname);

    Set<String> findAllLongNames();

    Map<EventPair, Count> getCounts(String logname, Set<EventPair> pairs);


}
