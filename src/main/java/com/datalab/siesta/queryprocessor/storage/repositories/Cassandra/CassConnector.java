package com.datalab.siesta.queryprocessor.storage.repositories.Cassandra;

import com.datalab.siesta.queryprocessor.model.Metadata;
import com.datalab.siesta.queryprocessor.storage.DatabaseRepository;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.data.cassandra.config.CassandraSessionFactoryBean;
import org.springframework.data.cassandra.config.CqlSessionFactoryBean;
import org.springframework.data.cassandra.config.SessionFactoryFactoryBean;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@ConditionalOnProperty(
        value = "database",
        havingValue = "cassandra",
        matchIfMissing = true
)
@Service
public class CassConnector implements DatabaseRepository {

    @Autowired
    private CqlSessionFactoryBean cb;


    @Override
    public Metadata getMetadata(String logname) {
        ResultSet x = cb.getObject().execute(String.format("select * from %s_meta",logname));
        Map<String,String> m= new HashMap<>();
        x.iterator().forEachRemaining(row->{
            m.put(row.getString(0),row.getString(1));
        });

        return new Metadata(m);
    }
}
