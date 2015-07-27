package com.contrastsecurity.cassandra.migration.service;

import com.contrastsecurity.cassandra.migration.dao.SchemaVersionDAO;
import com.contrastsecurity.cassandra.migration.info.*;
import com.contrastsecurity.cassandra.migration.resolver.MigrationResolver;

import java.util.*;

public class MigrationInfoService {

    private final SchemaVersionDAO schemaVersionDAO;

    private final MigrationResolver migrationResolver;

    private List<MigrationInfo> migrationInfos;

    private MigrationVersion target;

    public MigrationInfoService(MigrationResolver migrationResolver, SchemaVersionDAO schemaVersionDAO) {
        this.migrationResolver = migrationResolver;
        this.schemaVersionDAO = schemaVersionDAO;
    }

    public void load() {
        Collection<ResolvedMigration> availableMigrations = migrationResolver.resolveMigrations();
        List<AppliedMigration> appliedMigrations = schemaVersionDAO.findAppliedMigrations();

        migrationInfos = mergeAvailableAndAppliedMigrations(availableMigrations, appliedMigrations);

        if (MigrationVersion.CURRENT == target) {
            target = current().getVersion();
        }
    }

    List<MigrationInfo> mergeAvailableAndAppliedMigrations(Collection<ResolvedMigration> resolvedMigrations, List<AppliedMigration> appliedMigrations) {
        MigrationInfoContext context = new MigrationInfoContext();
        //context.pendingOrFuture = pendingOrFuture;
        context.target = target;

        Map<MigrationVersion, ResolvedMigration> resolvedMigrationsMap = new TreeMap<MigrationVersion, ResolvedMigration>();
        for (ResolvedMigration resolvedMigration : resolvedMigrations) {
            MigrationVersion version = resolvedMigration.getVersion();
            if (version.compareTo(context.lastResolved) > 0) {
                context.lastResolved = version;
            }
            resolvedMigrationsMap.put(version, resolvedMigration);
        }

        Map<MigrationVersion, AppliedMigration> appliedMigrationsMap = new TreeMap<MigrationVersion, AppliedMigration>();
        for (AppliedMigration appliedMigration : appliedMigrations) {
            MigrationVersion version = appliedMigration.getVersion();
            if (version.compareTo(context.lastApplied) > 0) {
                context.lastApplied = version;
            }
            appliedMigrationsMap.put(version, appliedMigration);
        }

        Set<MigrationVersion> allVersions = new HashSet<MigrationVersion>();
        allVersions.addAll(resolvedMigrationsMap.keySet());
        allVersions.addAll(appliedMigrationsMap.keySet());

        List<MigrationInfo> migrationInfos = new ArrayList<>();
        for (MigrationVersion version : allVersions) {
            ResolvedMigration resolvedMigration = resolvedMigrationsMap.get(version);
            AppliedMigration appliedMigration = appliedMigrationsMap.get(version);
            migrationInfos.add(new MigrationInfo(resolvedMigration, appliedMigration, context));
        }

        Collections.sort(migrationInfos);

        return migrationInfos;
    }

    public MigrationInfo current() {
        for (int i = migrationInfos.size() - 1; i >= 0; i--) {
            MigrationInfo migrationInfo = migrationInfos.get(i);
            if (migrationInfo.getState().isApplied()) {
                return migrationInfo;
            }
        }

        return null;
    }

    public MigrationInfo[] pending() {
        List<MigrationInfo> pendingMigrations = new ArrayList<MigrationInfo>();
        for (MigrationInfo migrationInfo : migrationInfos) {
            if (MigrationState.PENDING == migrationInfo.getState()) {
                pendingMigrations.add(migrationInfo);
            }
        }

        return pendingMigrations.toArray(new MigrationInfo[pendingMigrations.size()]);
    }

    /**
     * Retrieves the full set of infos about the migrations resolved on the classpath.
     *
     * @return The resolved migrations. An empty array if none.
     */
    public MigrationInfo[] resolved() {
        List<MigrationInfo> resolvedMigrations = new ArrayList<MigrationInfo>();
        for (MigrationInfo migrationInfo : migrationInfos) {
            if (migrationInfo.getState().isResolved()) {
                resolvedMigrations.add(migrationInfo);
            }
        }

        return resolvedMigrations.toArray(new MigrationInfo[resolvedMigrations.size()]);
    }

    /**
     * Retrieves the full set of infos about the migrations that failed.
     *
     * @return The failed migrations. An empty array if none.
     */
    public MigrationInfo[] failed() {
        List<MigrationInfo> failedMigrations = new ArrayList<MigrationInfo>();
        for (MigrationInfo migrationInfo : migrationInfos) {
            if (migrationInfo.getState().isFailed()) {
                failedMigrations.add(migrationInfo);
            }
        }

        return failedMigrations.toArray(new MigrationInfo[failedMigrations.size()]);
    }

    /**
     * Retrieves the full set of infos about future migrations applied to the DB.
     *
     * @return The future migrations. An empty array if none.
     */
    public MigrationInfo[] future() {
        List<MigrationInfo> futureMigrations = new ArrayList<MigrationInfo>();
        for (MigrationInfo migrationInfo : migrationInfos) {
            if ((migrationInfo.getState() == MigrationState.FUTURE_SUCCESS)
                    || (migrationInfo.getState() == MigrationState.FUTURE_FAILED)) {
                futureMigrations.add(migrationInfo);
            }
        }

        return futureMigrations.toArray(new MigrationInfo[futureMigrations.size()]);
    }
}
