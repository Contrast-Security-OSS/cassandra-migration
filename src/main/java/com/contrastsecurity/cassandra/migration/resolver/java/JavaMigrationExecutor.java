package com.contrastsecurity.cassandra.migration.resolver.java;

import com.contrastsecurity.cassandra.migration.CassandraMigrationException;
import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.contrastsecurity.cassandra.migration.resolver.MigrationExecutor;
import com.datastax.driver.core.Session;

/**
 * Adapter for executing migrations implementing JavaMigration.
 */
public class JavaMigrationExecutor implements MigrationExecutor {
    /**
     * The JavaMigration to execute.
     */
    private final JavaMigration javaMigration;

    /**
     * Creates a new JdbcMigrationExecutor.
     *
     * @param javaMigration The JdbcMigration to execute.
     */
    public JavaMigrationExecutor(JavaMigration javaMigration) {
        this.javaMigration = javaMigration;
    }

    @Override
    public void execute(Session session) {
        try {
            javaMigration.migrate(session);
        } catch (Exception e) {
            throw new CassandraMigrationException("Migration failed !", e);
        }
    }
}
