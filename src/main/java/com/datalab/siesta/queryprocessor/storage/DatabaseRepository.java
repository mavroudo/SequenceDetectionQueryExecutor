package com.datalab.siesta.queryprocessor.storage;

import com.datalab.siesta.queryprocessor.model.Metadata;

public interface DatabaseRepository {

    Metadata getMetadata(String logname);


}
