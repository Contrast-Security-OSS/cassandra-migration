package com.hubrick.cassandra.migration.resolver.java;

import com.hubrick.cassandra.migration.CassandraMigrationException;
import com.hubrick.cassandra.migration.api.JavaMigration;
import com.hubrick.cassandra.migration.resolver.MigrationExecutor;
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
