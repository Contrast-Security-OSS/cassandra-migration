package com.contrastsecurity.cassandra.migration.action;

import com.contrastsecurity.cassandra.migration.MigrationVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class Initialize {
    private Session session;
    private String keyspace;
    private String migrationVersionTable;

    public void run(Session session, MigrationVersion version, String keyspace) {
        this.session = session;
        this.keyspace = keyspace;
        this.migrationVersionTable = version.getTable();

        if(!migrationVersionTableExists())
            createMigrationVersionTable();
    }

    private void createMigrationVersionTable() {
        String ddl =
                "CREATE TABLE " + migrationVersionTable + "(" +
                "  rank bigint," +
                "  version text," +
                "  description text," +
                "  script text," +
                "  checksum bigint," +
                "  installed_by text," +
                "  installed_on timestamp," +
                "  execution_time bigint," +
                "  success boolean," +
                "  PRIMARY KEY (version, rank)" +
                ") with CLUSTERING ORDER BY (rank DESC);";
        session.execute(ddl);
    }

    private boolean migrationVersionTableExists() {
        Statement statement = QueryBuilder
                .select()
                .column("columnfamily_name")
                .from("System", "schema_columnfamilies")
                .where(eq("keyspace_name", keyspace))
                .and(eq("columnfamily_name", migrationVersionTable));
        ResultSet results = session.execute(statement);
        for ( Row row : results ) {
            String table = row.getString("columnfamily_name");
            if(null != table && table.equalsIgnoreCase(migrationVersionTable)) {
                return true;
            }
        }
        return false;
    }
}
