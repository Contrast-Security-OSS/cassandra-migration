package com.contrastsecurity.cassandra.migration.dao;

import com.contrastsecurity.cassandra.migration.config.Keyspace;
import com.contrastsecurity.cassandra.migration.config.MigrationType;
import com.contrastsecurity.cassandra.migration.info.AppliedMigration;
import com.contrastsecurity.cassandra.migration.info.MigrationVersion;
import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.util.ArrayList;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class SchemaVersionDAO {

    private static final Log LOG = LogFactory.getLog(SchemaVersionDAO.class);

    private Session session;
    private Keyspace keyspace;
    private String tableName;

    public SchemaVersionDAO(Session session, Keyspace keyspace, String tableName) {
        this.session = session;
        this.keyspace = keyspace;
        this.tableName = tableName;
    }

    public Keyspace getKeyspace() {
        return this.keyspace;
    }

    public void createTable() {
        String ddl =
                "CREATE TABLE " + tableName + "(" +
                        "  version text," +
                        "  description text," +
                        "  script text," +
                        "  checksum int," +
                        "  type text," +
                        "  installed_by text," +
                        "  installed_on timestamp," +
                        "  execution_time bigint," +
                        "  success boolean," +
                        "  PRIMARY KEY (version, installed_on)" +
                        ") with CLUSTERING ORDER BY (installed_on DESC);";
        session.execute(ddl);
    }

    public boolean tableExists() {
        Statement statement = QueryBuilder
                .select()
                .column("columnfamily_name")
                .from("System", "schema_columnfamilies")
                .where(eq("keyspace_name", keyspace.getName()))
                .and(eq("columnfamily_name", tableName));
        ResultSet results = session.execute(statement);
        for ( Row row : results ) {
            String table = row.getString("columnfamily_name");
            if(null != table && table.equalsIgnoreCase(tableName)) {
                return true;
            }
        }
        return false;
    }

    public List<AppliedMigration> findAppliedMigrations() {
        if (!tableExists()) {
            createTable();
            return new ArrayList<>();
        }

        Statement statement = QueryBuilder
                .select()
                .column("version")
                .column("description")
                .column("script")
                .column("checksum")
                .column("type")
                .column("installed_by")
                .column("installed_on")
                .column("execution_time")
                .column("success")
                .from(keyspace.getName(), tableName)
                .orderBy(QueryBuilder.asc("version"));

        List<AppliedMigration>appliedMigrations = new ArrayList<>();
        ResultSet results = session.execute(statement);
        for ( Row row : results ) {
            AppliedMigration appliedMigration = new AppliedMigration(
                    MigrationVersion.fromVersion(row.getString("version")),
                    row.getString("description"),
                    MigrationType.valueOf(row.getString("type")),
                    row.getString("script"),
                    row.getInt("checksum"),
                    row.getInt("execution_time"),
                    row.getBool("success")
            );
            appliedMigrations.add(appliedMigration);
        }
        return appliedMigrations;
    }

    public void addAppliedMigration(AppliedMigration appliedMigration) {
        if (!tableExists()) {
            createTable();
        }

        MigrationVersion version = appliedMigration.getVersion();
        Statement statement = QueryBuilder
                .insertInto(keyspace.getName(), tableName)
                .value("version", version.toString())
                .value("description", appliedMigration.getDescription())
                .value("type", appliedMigration.getType())
                .value("script", appliedMigration.getScript())
                .value("checksum", appliedMigration.getChecksum())
                .value("installed_by", appliedMigration.getInstalledBy())
                .value("execution_time", appliedMigration.getExecutionTime())
                .value("success", appliedMigration.isSuccess());

        LOG.debug("MetaData keyspace " + tableName + " successfully updated to reflect changes");
        addAppliedMigration(appliedMigration);
    }
}
