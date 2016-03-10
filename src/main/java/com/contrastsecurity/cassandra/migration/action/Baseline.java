package com.contrastsecurity.cassandra.migration.action;

import com.contrastsecurity.cassandra.migration.CassandraMigrationException;
import com.contrastsecurity.cassandra.migration.dao.SchemaVersionDAO;
import com.contrastsecurity.cassandra.migration.info.AppliedMigration;
import com.contrastsecurity.cassandra.migration.info.MigrationVersion;
import com.contrastsecurity.cassandra.migration.resolver.MigrationResolver;

public class Baseline {

    private SchemaVersionDAO schemaVersionDao;
    private MigrationResolver migrationResolver;
    private MigrationVersion baselineVersion;
    private String baselineDescription;

    public Baseline(SchemaVersionDAO schemaVersionDao, MigrationResolver migrationResolver, MigrationVersion baselineVersion, String baselineDescription) {
        this.schemaVersionDao = schemaVersionDao;
        this.migrationResolver = migrationResolver;
        this.baselineVersion = baselineVersion;
        this.baselineDescription = baselineDescription;
    }

    public void run() {
        AppliedMigration baselineMigration = schemaVersionDao.getBaselineMarker();
        if (schemaVersionDao.hasAppliedMigrations()) {
            throw new CassandraMigrationException("Unable to baseline metadata table " + schemaVersionDao.getTableName() + " as it already contains migrations");
        }
        if (schemaVersionDao.hasBaselineMarker()) {
            if (!baselineMigration.getVersion().equals(baselineVersion) || !baselineMigration.getDescription().equals(baselineDescription)) {
                throw new CassandraMigrationException("Unable to baseline metadata table " + schemaVersionDao.getTableName() + " with (" + baselineVersion +
                        "," + baselineDescription + ") as it has already been initialized with (" + baselineMigration.getVersion() + "," + baselineMigration
                        .getDescription() + ")");
            }
        } else {
            if (baselineVersion.equals(MigrationVersion.fromVersion("0"))) {
                throw new CassandraMigrationException("Unable to baseline metadata table " + schemaVersionDao.getTableName() + " with version 0 as this " +
                        "version was used for schema creation");
            }
            schemaVersionDao.addBaselineMarker(baselineVersion, baselineDescription);
        }
    }
}