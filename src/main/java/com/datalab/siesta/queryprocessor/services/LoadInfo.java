package com.datalab.siesta.queryprocessor.services;

import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * This class is running during initialization and loads information from the storage. Specifically, it loads the metadata
 * for all the log databases, and also for each log database loads the different event types.
 */
@Service
@ComponentScan
public class LoadInfo {

    private DBConnector dbConnector;


    @Autowired
    public LoadInfo(DBConnector dbConnector) {
        this.dbConnector = dbConnector;

    }

    @Bean
    public LoadedMetadata getAllMetadata() {
        Map<String, Metadata> m = new HashMap<>();
        for (String l : dbConnector.findAllLongNames()) {
//            TODO: determine a better way to find the starting ts
            List<Long> list = new ArrayList<>();
            list.add(0L);
            list.add(1L);

            Map<Long, List<EventBoth>> x = dbConnector.querySeqTable(l, list);
            Metadata metadata = dbConnector.getMetadata(l);
            if (x.containsKey(0L)) {
                metadata.setStart_ts(x.get(0L).get(0).getTimestamp().toString());
            } else if (x.containsKey(1L)){
                metadata.setStart_ts(x.get(1L).get(0).getTimestamp().toString());
            }
            else {
                metadata.setStart_ts("");
            }
            metadata.setLast_ts(metadata.getLast_interval().split("_")[0]);
            m.put(l, metadata);
        }
        return new LoadedMetadata(m);
    }

    @Bean
    public LoadedEventTypes getAllEventTypes() {
        Map<String, List<String>> response = new HashMap<>();
        for (String l : dbConnector.findAllLongNames()) {
            response.put(l, dbConnector.getEventNames(l));
        }
        return new LoadedEventTypes(response);
    }
}
