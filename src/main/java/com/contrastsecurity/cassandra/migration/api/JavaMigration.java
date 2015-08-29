package com.contrastsecurity.cassandra.migration.api;

import com.datastax.driver.core.Session;

public interface JavaMigration {
    void migrate(Session session) throws Exception;
}
