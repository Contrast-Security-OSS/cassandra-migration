package com.contrastsecurity.cassandra.migration.action;

import com.contrastsecurity.cassandra.migration.config.Keyspace;
import com.contrastsecurity.cassandra.migration.dao.SchemaVersionDAO;
import com.contrastsecurity.cassandra.migration.info.MigrationVersion;
import com.datastax.driver.core.Session;

public class Initialize {
    private Session session;
    private Keyspace keyspace;
    private String migrationVersionTable;

    public void run(Session session, Keyspace keyspace) {
        this.session = session;
        this.keyspace = keyspace;
        this.migrationVersionTable = MigrationVersion.EMPTY.getTable();

        SchemaVersionDAO dao = new SchemaVersionDAO(session, keyspace, migrationVersionTable);
        if (!dao.tableExists())
            dao.createTable();
    }
}
