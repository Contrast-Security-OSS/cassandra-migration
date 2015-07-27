package com.contrastsecurity.cassandra.migration.action;

import com.contrastsecurity.cassandra.migration.CassandraMigrationException;
import com.contrastsecurity.cassandra.migration.dao.SchemaVersionDAO;
import com.contrastsecurity.cassandra.migration.info.AppliedMigration;
import com.contrastsecurity.cassandra.migration.info.MigrationInfo;
import com.contrastsecurity.cassandra.migration.info.MigrationState;
import com.contrastsecurity.cassandra.migration.info.MigrationVersion;
import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;
import com.contrastsecurity.cassandra.migration.resolver.MigrationExecutor;
import com.contrastsecurity.cassandra.migration.resolver.MigrationResolver;
import com.contrastsecurity.cassandra.migration.service.MigrationInfoService;
import com.contrastsecurity.cassandra.migration.utils.StopWatch;

import java.sql.SQLException;

public class Migrate {
    private static final Log LOG = LogFactory.getLog(Migrate.class);

    private final SchemaVersionDAO schemaVersionDAO;
    private final MigrationResolver migrationResolver;

    public Migrate(MigrationResolver migrationResolver, SchemaVersionDAO schemaVersionDAO) {
        this.migrationResolver = migrationResolver;
        this.schemaVersionDAO = schemaVersionDAO;
    }

    public int run() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        int migrationSuccessCount = 0;
        while(true) {
            final boolean firstRun = migrationSuccessCount == 0;


            MigrationInfoService infoService = new MigrationInfoService(migrationResolver, schemaVersionDAO);
            infoService.load();

            MigrationVersion currentSchemaVersion = MigrationVersion.EMPTY;
            if (infoService.current() != null) {
                currentSchemaVersion = infoService.current().getVersion();
            }
            if (firstRun) {
                LOG.info("Current version of keyspace " + schemaVersionDAO.getKeyspace().getName() + ": " + currentSchemaVersion);
            }

            MigrationInfo[] future = infoService.future();
            if (future.length > 0) {
                MigrationInfo[] resolved = infoService.resolved();
                if (resolved.length == 0) {
                    LOG.warn("Keyspace " + schemaVersionDAO.getKeyspace().getName() + " has version " + currentSchemaVersion
                            + ", but no migration could be resolved in the configured locations !");
                } else {
                    LOG.warn("Keyspace " + schemaVersionDAO.getKeyspace().getName() + " has a version (" + currentSchemaVersion
                            + ") that is newer than the latest available migration ("
                            + resolved[resolved.length - 1].getVersion() + ") !");
                }
            }

            MigrationInfo[] failed = infoService.failed();
            if (failed.length > 0) {
                if ((failed.length == 1)
                        && (failed[0].getState() == MigrationState.FUTURE_FAILED)) {
                    LOG.warn("Keyspace " + schemaVersionDAO.getKeyspace().getName() + " contains a failed future migration to version " + failed[0].getVersion() + " !");
                } else {
                    throw new CassandraMigrationException("Keyspace " + schemaVersionDAO.getKeyspace().getName() + " contains a failed migration to version " + failed[0].getVersion() + " !");
                }
            }

            MigrationInfo[] pendingMigrations = infoService.pending();

            if (pendingMigrations.length == 0) {
                return migrationSuccessCount;
            }
            applyMigration(pendingMigrations[0]);

            migrationSuccessCount++;
        }
        stopWatch.stop();
        return 0;
    }

    private MigrationVersion applyMigration(final MigrationInfo migration) {
        MigrationVersion version = migration.getVersion();
        LOG.info("Migrating keyspace " + schemaVersionDAO.getKeyspace().getName() + " to version " + version +
                " - " + migration.getDescription());

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        try {
            final MigrationExecutor migrationExecutor = migration.getResolvedMigration().getExecutor();
            if (migrationExecutor.executeInTransaction()) {
                new TransactionTemplate(connectionUserObjects).execute(new TransactionCallback<Void>() {
                    public Void doInTransaction() throws SQLException {
                        migrationExecutor.execute(connectionUserObjects);
                        return null;
                    }
                });
            } else {
                try {
                    migrationExecutor.execute(connectionUserObjects);
                } catch (SQLException e) {
                    throw new CassandraMigrationException("Unable to apply migration", e);
                }
            }
            LOG.debug("Successfully completed and committed migration of keyspace " +
                    schemaVersionDAO.getKeyspace().getName() + " to version " + version);
        } catch (CassandraMigrationException e) {
            String failedMsg = "Migration of keyspace " + schemaVersionDAO.getKeyspace().getName() +
                    " to version " + version + " failed!";

            LOG.error(failedMsg + " Please restore backups and roll back database and code!");

            stopWatch.stop();
            int executionTime = (int) stopWatch.getTotalTimeMillis();
            AppliedMigration appliedMigration = new AppliedMigration(version, migration.getDescription(),
                    migration.getType(), migration.getScript(), migration.getChecksum(), executionTime, false);
            schemaVersionDAO.addAppliedMigration(appliedMigration);
            throw e;
        }

        stopWatch.stop();
        int executionTime = (int) stopWatch.getTotalTimeMillis();

        AppliedMigration appliedMigration = new AppliedMigration(version, migration.getDescription(),
                migration.getType(), migration.getScript(), migration.getChecksum(), executionTime, true);
        metaDataTable.addAppliedMigration(appliedMigration);

        return version;
    }
}
