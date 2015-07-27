package com.contrastsecurity.cassandra.migration.resolver.cql;

import com.contrastsecurity.cassandra.migration.config.MigrationType;
import com.contrastsecurity.cassandra.migration.config.ScriptsLocation;
import com.contrastsecurity.cassandra.migration.resolver.MigrationInfoHelper;
import com.contrastsecurity.cassandra.migration.resolver.MigrationResolver;
import com.contrastsecurity.cassandra.migration.resolver.ResolvedMigrationComparator;
import com.contrastsecurity.cassandra.migration.info.MigrationVersion;
import com.contrastsecurity.cassandra.migration.info.ResolvedMigration;
import com.contrastsecurity.cassandra.migration.utils.Pair;
import com.contrastsecurity.cassandra.migration.utils.scanner.Resource;
import com.contrastsecurity.cassandra.migration.utils.scanner.Scanner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Migration resolver for sql files on the classpath. The sql files must have names like
 * V1__Description.sql or V1_1__Description.sql.
 */
public class CqlMigrationResolver implements MigrationResolver {

    /**
     * The scanner to use.
     */
    private final Scanner scanner;

    /**
     * The base directory on the classpath where to migrations are located.
     */
    private final ScriptsLocation location;

    /**
     * The prefix for sql migrations
     */
    private final String sqlMigrationPrefix;

    /**
     * The separator for sql migrations
     */
    private final String sqlMigrationSeparator;

    /**
     * The suffix for sql migrations
     */
    private final String sqlMigrationSuffix;

    /**
     * Creates a new instance.
     *
     * @param classLoader           The ClassLoader for loading migrations on the classpath.
     * @param location              The location on the classpath where to migrations are located.
     * @param sqlMigrationPrefix    The prefix for sql migrations
     * @param sqlMigrationSeparator The separator for sql migrations
     * @param sqlMigrationSuffix    The suffix for sql migrations
     */
    public CqlMigrationResolver(ClassLoader classLoader, ScriptsLocation location,
                                String sqlMigrationPrefix, String sqlMigrationSeparator, String sqlMigrationSuffix) {
        this.scanner = new Scanner(classLoader);
        this.location = location;
        this.sqlMigrationPrefix = sqlMigrationPrefix;
        this.sqlMigrationSeparator = sqlMigrationSeparator;
        this.sqlMigrationSuffix = sqlMigrationSuffix;
    }

    public List<ResolvedMigration> resolveMigrations() {
        List<ResolvedMigration> migrations = new ArrayList<>();

        Resource[] resources = scanner.scanForResources(location, sqlMigrationPrefix, sqlMigrationSuffix);
        for (Resource resource : resources) {
            ResolvedMigration resolvedMigration = extractMigrationInfo(resource);
            resolvedMigration.setPhysicalLocation(resource.getLocationOnDisk());

            migrations.add(resolvedMigration);
        }

        Collections.sort(migrations, new ResolvedMigrationComparator());
        return migrations;
    }

    /**
     * Extracts the migration info for this resource.
     *
     * @param resource The resource to analyse.
     * @return The migration info.
     */
    private ResolvedMigration extractMigrationInfo(Resource resource) {
        ResolvedMigration migration = new ResolvedMigration();

        Pair<MigrationVersion, String> info =
                MigrationInfoHelper.extractVersionAndDescription(resource.getFilename(),
                        sqlMigrationPrefix, sqlMigrationSeparator, sqlMigrationSuffix);
        migration.setVersion(info.getLeft());
        migration.setDescription(info.getRight());

        migration.setScript(extractScriptName(resource));

        migration.setChecksum(calculateChecksum(resource.loadAsBytes()));
        migration.setType(MigrationType.CQL);
        return migration;
    }

    /**
     * Extracts the script name from this resource.
     *
     * @param resource The resource to process.
     * @return The script name.
     */
    /* private -> for testing */ String extractScriptName(Resource resource) {
        if (location.getPath().isEmpty()) {
            return resource.getLocation();
        }

        return resource.getLocation().substring(location.getPath().length() + 1);
    }

    /**
     * Calculates the checksum of these bytes.
     *
     * @param bytes The bytes to calculate the checksum for.
     * @return The crc-32 checksum of the bytes.
     */
    private static int calculateChecksum(byte[] bytes) {
        final CRC32 crc32 = new CRC32();
        crc32.update(bytes);
        return (int) crc32.getValue();
    }
}
