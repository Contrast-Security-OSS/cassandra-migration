package com.contrastsecurity.cassandra.migration.dao;

import com.contrastsecurity.cassandra.migration.CassandraMigrationException;
import com.contrastsecurity.cassandra.migration.config.Keyspace;
import com.contrastsecurity.cassandra.migration.config.MigrationConfigs;
import com.contrastsecurity.cassandra.migration.config.MigrationType;
import com.contrastsecurity.cassandra.migration.info.AppliedMigration;
import com.contrastsecurity.cassandra.migration.info.MigrationVersion;
import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;
import com.contrastsecurity.cassandra.migration.utils.CachePrepareStatement;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.Metadata;

import java.time.Instant;
import java.time.ZoneId;
import java.util.*;

import static com.contrastsecurity.cassandra.migration.utils.Ensure.notNull;
import static java.lang.String.format;

public class SchemaVersionDAO {

    private static final Log LOG = LogFactory.getLog(SchemaVersionDAO.class);
    private static final String COUNTS_TABLE_NAME_SUFFIX = "_counts";
    private final Keyspace keyspace;
    private final String tableName;
    private final String keyspaceName;
    private final String tableCountName;
    private final String executionProfileName;
    private final CachePrepareStatement cachePs;
    private final CqlSession session;
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.QUORUM;

    /**
     * The name of the table that manages the migration scripts
     */
    private static final String SCHEMA_CF = "schema_migration";
    /**
     * Statement used to create the table that manages the migrations.
     */
    private static final String CREATE_MIGRATION_CF = "CREATE TABLE IF NOT EXISTS %s"
            + " (version_rank int, installed_rank int, version text, description text,"
            + " script text, checksum int, type text, installed_by text, installed_on timestamp, "
            + " execution_time int, success boolean, PRIMARY KEY (version))";

    private static final String CREATE_MIGRATION_COUNT_CF = "CREATE TABLE IF NOT EXISTS %s" +
            " (name text, count counter, PRIMARY KEY (name))";

    /**
     * Insert statement that logs a migration into the schema_migration table.
     */
    private static final String ADD_MIGRATION = "insert into %s"
            + "(version_rank, installed_rank, version, description, " +
              "type, script, checksum, installed_on, installed_by, execution_time, success) values" +
            "(?, ?, ?, ?, ?, ?, ?, dateOf(now()), ?, ?, ?)";
    private static final String UPDATE_MIGRATION_COUNT = "update %s " +
            "set count = count + 1 where name = 'installed_rank'";
    private static final String UPDATE_MIGRATION_VERSION_RANK = "update %s " +
            "set version_rank = ? where version = ?";
    private static final String SELECT_COUNT_MIGRATION = "select count from %s " +
            "where name = 'installed_rank'";
    private static final String SELECT_MIGRATION = "select version, version_rank from %s";
    /**
     * The query that retrieves current schema version
     */
    private static final String VERSION_QUERY = "select version_rank, installed_rank, version, description, " +
            "type, script, checksum, installed_on, installed_by, execution_time, success from %s";


    public SchemaVersionDAO(CqlSession session, Keyspace keyspace) {
        this(session, new MigrationConfigs(keyspace));
    }

    public SchemaVersionDAO(CqlSession session, MigrationConfigs configuration) {
        this.session = notNull(session, "session");
        if (!configuration.isValid()) {
            throw new IllegalArgumentException("The provided configuration is invalid. Please check if all required values are" +
                    " available. Current configuration is: " + System.lineSeparator() + configuration);
        }
        this.keyspace = configuration.getKeyspace();
        this.cachePs = new CachePrepareStatement(session);
        //If running on a single host, don't force ConsistencyLevel.ALL
        this.consistencyLevel =
                session.getMetadata().getNodes().size() > 1 ? ConsistencyLevel.ALL :  ConsistencyLevel.ONE;
        this.keyspaceName = keyspace.getName();
        this.executionProfileName = configuration.getExecutionProfile();
        this.tableName = createTableName(configuration.getTablePrefix(), SCHEMA_CF);
        this.tableCountName = createTableName(configuration.getTablePrefix(), SCHEMA_CF + COUNTS_TABLE_NAME_SUFFIX);
        createKeyspaceIfRequired();
        useKeyspace();
        ensureSchemaTable();
    }

    public Keyspace getKeyspace() {
        return this.keyspace;
    }

    public void createTablesIfNotExist() {
        if (tablesExist()) {
            return;
        }
        createSchemaTable();
    }

    /**
     * Inserts the result of the migration into the migration table
     *
     * @param appliedMigration the migration that was executed
     */
    public void addAppliedMigration(AppliedMigration appliedMigration) {
        createTablesIfNotExist();
        MigrationVersion version = appliedMigration.getVersion();
        int versionRank = calculateVersionRank(version);
        PreparedStatement addMigrationStatement = cachePs.prepare(format(ADD_MIGRATION, getTableName()));
        BoundStatement boundStatement = addMigrationStatement.bind(versionRank,
                calculateInstalledRank(),
                version.toString(),
                appliedMigration.getDescription(),
                appliedMigration.getType().name(),
                appliedMigration.getScript(),
                appliedMigration.getChecksum(),
                appliedMigration.getInstalledBy(),
                appliedMigration.getExecutionTime(),
                appliedMigration.isSuccess());
        executeStatement(boundStatement, this.consistencyLevel);
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
        ResultSet resultSet = executeStatement(format(VERSION_QUERY, getTableName()));
        List<AppliedMigration> resultsList = new ArrayList<>();
        for (Row row : resultSet) {
            Instant instant = row.getLocalDate("installed_on").atStartOfDay(ZoneId.systemDefault()).toInstant();
            resultsList.add(new AppliedMigration(
                    row.getInt("version_rank"),
                    row.getInt("installed_rank"),
                    MigrationVersion.fromVersion(row.getString("version")),
                    row.getString("description"),
                    MigrationType.valueOf(row.getString("type")),
                    row.getString("script"),
                    row.isNull("checksum") ? null : row.getInt("checksum"),
                    Date.from(instant),
                    row.getString("installed_by"),
                    row.getInt("execution_time"),
                    row.getBool("success")
            ));
        }
        return resultsList;
    }

    /**
     * Calculates the installed rank for the new migration to be inserted.
     *
     * @return The installed rank.
     */
    private int calculateInstalledRank() {
        Statement statement = SimpleStatement.newInstance(format(UPDATE_MIGRATION_COUNT, getTableCountName()));
        executeStatement(statement, consistencyLevel);
        ResultSet result = executeStatement(format(SELECT_COUNT_MIGRATION, getTableCountName()));
        return (int) result.one().getLong("count");
    }

    static class MigrationMetaHolder {
        private final int versionRank;

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
        ResultSet versionRows = executeStatement(format(SELECT_MIGRATION, getTableName()));

        List<MigrationVersion> migrationVersions = new ArrayList<>();
        HashMap<String, MigrationMetaHolder> migrationMetaHolders = new HashMap<>();
        for (Row versionRow : versionRows) {
            migrationVersions.add(MigrationVersion.fromVersion(versionRow.getString("version")));
            migrationMetaHolders.put(versionRow.getString("version"), new MigrationMetaHolder(
                    versionRow.getInt("version_rank")
            ));
        }

        Collections.sort(migrationVersions);

        BatchStatement batchStatement = BatchStatement.newInstance(BatchType.LOGGED);
        PreparedStatement preparedStatement = cachePs.prepare(format(UPDATE_MIGRATION_VERSION_RANK, tableName));

        for (int i = 0; i < migrationVersions.size(); i++) {
            if (version.compareTo(migrationVersions.get(i)) < 0) {
                for (int z = i; z < migrationVersions.size(); z++) {
                    String migrationVersionStr = migrationVersions.get(z).getVersion();
                    batchStatement.add(preparedStatement.bind(
                            migrationMetaHolders.get(migrationVersionStr).getVersionRank() + 1,
                            migrationVersionStr));
                }
                return i + 1;
            }
        }
        executeStatement(batchStatement, consistencyLevel);

        return migrationVersions.size() + 1;
    }
    private ResultSet executeStatement(String statement) throws DriverException {
        return executeStatement(SimpleStatement.newInstance(statement), this.consistencyLevel);
    }

    private ResultSet executeStatement(Statement<?> statement, ConsistencyLevel consistencyLevel) throws DriverException {
        return session.execute(statement
                .setExecutionProfileName(executionProfileName)
                .setConsistencyLevel(consistencyLevel));
    }

    private void useKeyspace() {
        LOG.info("Changing keyspace of the session to '" + keyspaceName + "'");
        session.execute("USE " + keyspaceName);
    }

    private static String createTableName(String tablePrefix, String tableName) {
        if (tablePrefix == null || tablePrefix.isEmpty()) {
            return tableName;
        }
        return String.format("%s_%s", tablePrefix, tableName);
    }

    private void createKeyspaceIfRequired() {
        if (keyspace == null || keyspaceExists()) {
            return;
        }
        try {
            executeStatement(this.keyspace.getCqlStatement());
        } catch (DriverException exception) {
            throw new CassandraMigrationException(format("Unable to create keyspace %s.", keyspaceName), exception);
        }
    }

    /**
     * Makes sure the schema migration table exists. If it is not available it will be created.
     */
    private void ensureSchemaTable() {
        if (tablesExist()) {
            return;
        }
        createSchemaTable();
    }

    private boolean tablesExist() {
        Metadata metadata = session.getMetadata();

        return isTableExisting(metadata, tableName)
                && isTableExisting(metadata, tableCountName);
    }

    private boolean isTableExisting(Metadata metadata, String tableName) {
        return metadata
                .getKeyspace(keyspaceName)
                .map(keyspaceMetadata -> keyspaceMetadata.getTable(tableName).isPresent())
                .orElse(false);
    }

    private void createSchemaTable() {
        executeStatement(format(CREATE_MIGRATION_CF, getTableName()));
        executeStatement(format(CREATE_MIGRATION_COUNT_CF, getTableCountName()));
    }


    private boolean keyspaceExists() {
        return session.getMetadata().getKeyspace(keyspaceName).isPresent();
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableCountName() {
        return tableCountName;
    }
}
