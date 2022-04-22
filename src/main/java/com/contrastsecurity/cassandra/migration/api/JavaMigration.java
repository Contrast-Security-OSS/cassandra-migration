package com.contrastsecurity.cassandra.migration.api;

import com.datastax.driver.core.Session;
import com.datastax.oss.driver.api.core.CqlSession;

public interface JavaMigration {
    void migrate(CqlSession session) throws Exception;
}
