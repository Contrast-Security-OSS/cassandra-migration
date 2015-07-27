package com.contrastsecurity.cassandra.migration.resolver;

import com.contrastsecurity.cassandra.migration.info.ResolvedMigration;

import java.util.Collection;

public interface MigrationResolver {
    Collection<ResolvedMigration> resolveMigrations();
}
