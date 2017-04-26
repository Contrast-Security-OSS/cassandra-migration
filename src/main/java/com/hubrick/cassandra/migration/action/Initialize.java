package com.hubrick.cassandra.migration.action;

import com.hubrick.cassandra.migration.config.Keyspace;
import com.hubrick.cassandra.migration.dao.SchemaVersionDAO;
import com.datastax.driver.core.Session;

public class Initialize {

    public void run(Session session, Keyspace keyspace, String migrationVersionTableName) {
        SchemaVersionDAO dao = new SchemaVersionDAO(session, keyspace, migrationVersionTableName);
        dao.createTablesIfNotExist();
    }
}
