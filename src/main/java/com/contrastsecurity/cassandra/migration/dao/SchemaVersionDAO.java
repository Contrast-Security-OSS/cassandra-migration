package com.contrastsecurity.cassandra.migration.dao;

import com.contrastsecurity.cassandra.migration.config.Keyspace;
import com.contrastsecurity.cassandra.migration.config.MigrationType;
import com.contrastsecurity.cassandra.migration.info.AppliedMigration;
import com.contrastsecurity.cassandra.migration.info.MigrationVersion;
import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class SchemaVersionDAO {

    private static final Log LOG = LogFactory.getLog(SchemaVersionDAO.class);
    private static final String COUNTS_TABLE_NAME_SUFFIX = "_counts";

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

    public void createTablesIfNotExist() {
        if (tablesExist()) {
            return;
        }

        Statement statement = new SimpleStatement(
                "CREATE TABLE IF NOT EXISTS " + keyspace.getName() + "." + tableName + "(" +
                        "  version_rank int," +
                        "  installed_rank int," +
                        "  version text," +
                        "  description text," +
                        "  script text," +
                        "  checksum int," +
                        "  type text," +
                        "  installed_by text," +
                        "  installed_on timestamp," +
                        "  execution_time int," +
                        "  success boolean," +
                        "  PRIMARY KEY (type, version_rank)" +
                        ")");
        statement.setConsistencyLevel(ConsistencyLevel.ALL);
        session.execute(statement);

        statement = new SimpleStatement(
                "CREATE INDEX IF NOT EXISTS " + keyspace.getName() + "." + tableName + "_version" +
                        " ON " + tableName + " (version)");
        session.executeAsync(statement);

        statement = new SimpleStatement(
                "CREATE TABLE IF NOT EXISTS " + keyspace.getName() + "." + tableName + COUNTS_TABLE_NAME_SUFFIX + " (" +
                        "  name text," +
                        "  count counter," +
                        "  PRIMARY KEY (name)" +
                        ");");
        statement.setConsistencyLevel(ConsistencyLevel.ALL);
        session.execute(statement);
    }

    public boolean tablesExist() {
        boolean schemaVersionTableExists = false;
        boolean schemaVersionCountsTableExists = false;

        Statement statement = QueryBuilder
                .select()
                .column("columnfamily_name")
                .from("System", "schema_columnfamilies")
                .where(eq("keyspace_name", keyspace.getName()))
                .and(in("columnfamily_name", tableName, tableName + COUNTS_TABLE_NAME_SUFFIX));
        statement.setConsistencyLevel(ConsistencyLevel.ALL);
        ResultSet results = session.execute(statement);
        for (Row row : results) {
            String table = row.getString("columnfamily_name");
            if (null != table && table.equalsIgnoreCase(tableName)) {
                schemaVersionTableExists = true;
            }
            if (null != table && table.equalsIgnoreCase(tableName + COUNTS_TABLE_NAME_SUFFIX)) {
                schemaVersionCountsTableExists = true;
            }
        }
        return schemaVersionTableExists && schemaVersionCountsTableExists;
    }

    public void addAppliedMigration(AppliedMigration appliedMigration) {
        createTablesIfNotExist();

        MigrationVersion version = appliedMigration.getVersion();

        int versionRank = calculateVersionRank(version);

        Select select = QueryBuilder
                .select("version", "version_rank")
                .from(keyspace.getName(), tableName);
        select.where(gte("version_rank", versionRank));
        select.allowFiltering();
        select.setConsistencyLevel(ConsistencyLevel.ALL);
        ResultSet results = session.execute(select);
        for (Row row : results) {
            /*
             * can't do this in a single statement b/c increment is not supported unless the data type is
             * counter and you can not use gte on counter columns
             * TODO: do this as a Java Driver's batch statement
             */
            Update update = QueryBuilder
                    .update(keyspace.getName(), tableName);
            update.with(set("version_rank", row.getInt("version_rank") + 1));
            update.where(eq("version", row.getString("version")));
            update.setConsistencyLevel(ConsistencyLevel.ALL);
            session.execute(update);
        }

        PreparedStatement statement = session.prepare(
                "INSERT INTO " + keyspace.getName() + "." + tableName +
                        " (version_rank, installed_rank, version, description, type, script, checksum, installed_on," +
                        "  installed_by, execution_time, success)" +
                        " VALUES" +
                        " (?, ?, ?, ?, ?, ?, ?, dateOf(now()), ?, ?, ?);"
        );
        statement.setConsistencyLevel(ConsistencyLevel.ALL);
        session.execute(statement.bind(
                versionRank,
                calculateInstalledRank(),
                version.toString(),
                appliedMigration.getDescription(),
                appliedMigration.getType().name(),
                appliedMigration.getScript(),
                appliedMigration.getChecksum(),
                appliedMigration.getInstalledBy(),
                appliedMigration.getExecutionTime(),
                appliedMigration.isSuccess()
        ));
        LOG.debug("Schema version table " + tableName + " successfully updated to reflect changes");
    }

    /**
     * Retrieve the applied migrations from the metadata table.
     *
     * @param migrationTypes The specific migration types to look for. (Optional) None means find all migrations.
     * @return The applied migrations.
     */
    public List<AppliedMigration> findAppliedMigrations(MigrationType... migrationTypes) {
        if (!tablesExist()) {
            return new ArrayList<AppliedMigration>();
        }

        Select select = QueryBuilder
                .select()
                .column("version_rank")
                .column("installed_rank")
                .column("version")
                .column("description")
                .column("type")
                .column("script")
                .column("checksum")
                .column("installed_on")
                .column("installed_by")
                .column("execution_time")
                .column("success")
                .from(keyspace.getName(), tableName);
        if (migrationTypes.length > 0) {
            select.where(in("type", migrationTypes));
        }
        select.setConsistencyLevel(ConsistencyLevel.ALL);
        ResultSet results = session.execute(select);
        List<AppliedMigration> resultsList = new ArrayList<AppliedMigration>();
        for (Row row : results) {
            resultsList.add(new AppliedMigration(
                    row.getInt("version_rank"),
                    row.getInt("installed_rank"),
                    MigrationVersion.fromVersion(row.getString("version")),
                    row.getString("description"),
                    MigrationType.valueOf(row.getString("type")),
                    row.getString("script"),
                    row.getInt("checksum"),
                    row.getDate("installed_on"),
                    row.getString("installed_by"),
                    row.getInt("execution_time"),
                    row.getBool("success")
            ));
        }

        //order by version_rank not necessary here as it eventually gets saved in TreeMap that uses natural ordering

        return resultsList;
    }

    /**
     * Calculates the installed rank for the new migration to be inserted.
     *
     * @return The installed rank.
     */
    private int calculateInstalledRank() {
        Statement statement = new SimpleStatement(
                "UPDATE " + keyspace.getName() + "." + tableName + COUNTS_TABLE_NAME_SUFFIX +
                        " SET count = count + 1" +
                        "WHERE name = 'installed_rank';");
        session.execute(statement);
        Select select = QueryBuilder
                .select("count")
                .from(tableName + COUNTS_TABLE_NAME_SUFFIX);
        select.where(eq("name", "installed_rank"));
        select.setConsistencyLevel(ConsistencyLevel.ALL);
        ResultSet result = session.execute(select);
        return (int)result.one().getLong("count");
    }

    /**
     * Calculate the rank for this new version about to be inserted.
     *
     * @param version The version to calculated for.
     * @return The rank.
     */
    private int calculateVersionRank(MigrationVersion version) {
        Statement statement = QueryBuilder
                .select()
                .column("version")
                .from(keyspace.getName(), tableName);
        statement.setConsistencyLevel(ConsistencyLevel.ALL);
        ResultSet versionRows = session.execute(statement);

        List<MigrationVersion> migrationVersions = new ArrayList<MigrationVersion>();
        for (Row versionRow : versionRows) {
            migrationVersions.add(MigrationVersion.fromVersion(versionRow.getString("version")));
        }

        Collections.sort(migrationVersions);

        for (int i = 0; i < migrationVersions.size(); i++) {
            if (version.compareTo(migrationVersions.get(i)) < 0) {
                return i + 1;
            }
        }

        return migrationVersions.size() + 1;
    }
}
