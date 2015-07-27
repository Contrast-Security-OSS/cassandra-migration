package com.contrastsecurity.cassandra.migration.resolver;

import com.datastax.driver.core.Session;

/**
 * Executes a migration.
 */
public interface MigrationExecutor {
    void execute(Session session);
}
