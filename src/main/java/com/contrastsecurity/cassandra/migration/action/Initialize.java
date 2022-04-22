package com.contrastsecurity.cassandra.migration.action;

import com.contrastsecurity.cassandra.migration.config.Keyspace;
import com.contrastsecurity.cassandra.migration.dao.SchemaVersionDAO;
import com.datastax.driver.core.Session;
import com.datastax.oss.driver.api.core.CqlSession;

public class Initialize {

    public void run(CqlSession session, Keyspace keyspace) {
        SchemaVersionDAO dao = new SchemaVersionDAO(session, keyspace);
        dao.createTablesIfNotExist();
    }
}
