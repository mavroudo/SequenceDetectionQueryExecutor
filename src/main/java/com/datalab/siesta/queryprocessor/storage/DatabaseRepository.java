package com.datalab.siesta.queryprocessor.storage;

import com.datalab.siesta.queryprocessor.model.Metadata;

import java.util.List;
import java.util.Set;

public interface DatabaseRepository {

    Metadata getMetadata(String logname);

    Set<String> findAllLongNames();


}
