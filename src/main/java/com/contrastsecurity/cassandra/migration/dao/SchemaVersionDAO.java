package com.contrastsecurity.cassandra.migration.dao;

import com.contrastsecurity.cassandra.migration.config.Keyspace;
import com.contrastsecurity.cassandra.migration.config.MigrationType;
import com.contrastsecurity.cassandra.migration.info.AppliedMigration;
import com.contrastsecurity.cassandra.migration.info.MigrationVersion;
import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;
import com.contrastsecurity.cassandra.migration.utils.CachePrepareStatement;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class SchemaVersionDAO {

    private static final Log LOG = LogFactory.getLog(SchemaVersionDAO.class);
    private static final String COUNTS_TABLE_NAME_SUFFIX = "_counts";

    private Session session;
    private Keyspace keyspace;
    private String tableName;
    private CachePrepareStatement cachePs;
    private ConsistencyLevel consistencyLevel;

    public SchemaVersionDAO(Session session, Keyspace keyspace, String tableName) {
        this.session = session;
        this.keyspace = keyspace;
        this.tableName = tableName;
        this.cachePs = new CachePrepareStatement(session);
        //If running on a single host, don't force ConsistencyLevel.ALL
        this.consistencyLevel =
                session.getCluster().getMetadata().getAllHosts().size() > 1 ? ConsistencyLevel.ALL :  ConsistencyLevel.ONE;
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
                        "  PRIMARY KEY (version)" +
                        ");");
        statement.setConsistencyLevel(this.consistencyLevel);
        session.execute(statement);

        statement = new SimpleStatement(
                "CREATE TABLE IF NOT EXISTS " + keyspace.getName() + "." + tableName + COUNTS_TABLE_NAME_SUFFIX + " (" +
                        "  name text," +
                        "  count counter," +
                        "  PRIMARY KEY (name)" +
                        ");");
        statement.setConsistencyLevel(this.consistencyLevel);
        session.execute(statement);
    }

    public boolean tablesExist() {
        boolean schemaVersionTableExists = false;
        boolean schemaVersionCountsTableExists = false;

        Statement schemaVersionStatement = QueryBuilder
                .select()
                .countAll()
                .from(keyspace.getName(), tableName);

        Statement schemaVersionCountsStatement = QueryBuilder
                .select()
                .countAll()
                .from(keyspace.getName(), tableName + COUNTS_TABLE_NAME_SUFFIX);

        schemaVersionStatement.setConsistencyLevel(this.consistencyLevel);
        schemaVersionCountsStatement.setConsistencyLevel(this.consistencyLevel);

        try {
            ResultSet resultsSchemaVersion = session.execute(schemaVersionStatement);
            if (resultsSchemaVersion.one() != null) {
                schemaVersionTableExists = true;
            }
        } catch (InvalidQueryException e) {
            LOG.debug("No schema version table found with a name of " + tableName);
        }

        try {
            ResultSet resultsSchemaVersionCounts = session.execute(schemaVersionCountsStatement);
            if (resultsSchemaVersionCounts.one() != null) {
                schemaVersionCountsTableExists = true;
            }
        } catch (InvalidQueryException e) {
            LOG.debug("No schema version counts table found with a name of " + tableName + COUNTS_TABLE_NAME_SUFFIX);
        }

        return schemaVersionTableExists && schemaVersionCountsTableExists;
    }

    public void addAppliedMigration(AppliedMigration appliedMigration) {
        createTablesIfNotExist();

        MigrationVersion version = appliedMigration.getVersion();

        int versionRank = calculateVersionRank(version);
        PreparedStatement statement = cachePs.prepare(
                "INSERT INTO " + keyspace.getName() + "." + tableName +
                        " (version_rank, installed_rank, version, description, type, script, checksum, installed_on," +
                        "  installed_by, execution_time, success)" +
                        " VALUES" +
                        " (?, ?, ?, ?, ?, ?, ?, dateOf(now()), ?, ?, ?);"
        );

        statement.setConsistencyLevel(this.consistencyLevel);
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
     * @return The applied migrations.
     */
    public List<AppliedMigration> findAppliedMigrations() {
        if (!tablesExist()) {
            return new ArrayList<>();
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

        select.setConsistencyLevel(this.consistencyLevel);
        ResultSet results = session.execute(select);
        List<AppliedMigration> resultsList = new ArrayList<>();
        for (Row row : results) {
            resultsList.add(new AppliedMigration(
                    row.getInt("version_rank"),
                    row.getInt("installed_rank"),
                    MigrationVersion.fromVersion(row.getString("version")),
                    row.getString("description"),
                    MigrationType.valueOf(row.getString("type")),
                    row.getString("script"),
                    row.isNull("checksum") ? null : row.getInt("checksum"),
                    row.getTimestamp("installed_on"),
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
        select.setConsistencyLevel(this.consistencyLevel);
        ResultSet result = session.execute(select);
        return (int) result.one().getLong("count");
    }

    class MigrationMetaHolder {
        private int versionRank;

        public MigrationMetaHolder(int versionRank) {
            this.versionRank = versionRank;
        }

        public int getVersionRank() {
            return versionRank;
        }
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
                .column("version_rank")
                .from(keyspace.getName(), tableName);
        statement.setConsistencyLevel(this.consistencyLevel);
        ResultSet versionRows = session.execute(statement);

        List<MigrationVersion> migrationVersions = new ArrayList<>();
        HashMap<String, MigrationMetaHolder> migrationMetaHolders = new HashMap<>();
        for (Row versionRow : versionRows) {
            migrationVersions.add(MigrationVersion.fromVersion(versionRow.getString("version")));
            migrationMetaHolders.put(versionRow.getString("version"), new MigrationMetaHolder(
                    versionRow.getInt("version_rank")
            ));
        }

        Collections.sort(migrationVersions);

        BatchStatement batchStatement = new BatchStatement();
        PreparedStatement preparedStatement = cachePs.prepare(
                "UPDATE " + keyspace.getName() + "." + tableName +
                        " SET version_rank = ?" +
                        " WHERE version = ?;");

        for (int i = 0; i < migrationVersions.size(); i++) {
            if (version.compareTo(migrationVersions.get(i)) < 0) {
                for (int z = i; z < migrationVersions.size(); z++) {
                    String migrationVersionStr = migrationVersions.get(z).getVersion();
                    batchStatement.add(preparedStatement.bind(
                            migrationMetaHolders.get(migrationVersionStr).getVersionRank() + 1,
                            migrationVersionStr));
                    batchStatement.setConsistencyLevel(this.consistencyLevel);
                }
                return i + 1;
            }
        }
        session.execute(batchStatement);

        return migrationVersions.size() + 1;
    }
}
