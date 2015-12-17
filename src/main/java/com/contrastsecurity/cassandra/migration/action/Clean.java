package com.contrastsecurity.cassandra.migration.action;

import com.contrastsecurity.cassandra.migration.config.Keyspace;
import com.contrastsecurity.cassandra.migration.dao.SchemaVersionDAO;
import com.datastax.driver.core.Session;

/**
 * Created by amahakode on 15/12/15.
 */
public class Clean {

    public int run(Session session, Keyspace keyspace) {
        SchemaVersionDAO dao = new SchemaVersionDAO(session, keyspace);
        return dao.cleanTable();
    }
}


