package com.contrastsecurity.cassandra.migration.resolver;

import com.contrastsecurity.cassandra.migration.info.ResolvedMigration;

import java.util.Comparator;

public class ResolvedMigrationComparator implements Comparator<ResolvedMigration> {
    @Override
    public int compare(ResolvedMigration o1, ResolvedMigration o2) {
        return o1.getVersion().compareTo(o2.getVersion());
    }
}
