package com.contrastsecurity.cassandra.migration.resolver;

import com.contrastsecurity.cassandra.migration.CassandraMigrationException;
import com.contrastsecurity.cassandra.migration.info.MigrationVersion;
import com.contrastsecurity.cassandra.migration.utils.Pair;

/**
 * Parsing support for migrations that use the standard Flyway version + description embedding in their name. These
 * migrations have names like 1_2__Description .
 */
public class MigrationInfoHelper {
    /**
     * Prevents instantiation.
     */
    private MigrationInfoHelper() {
        //Do nothing.
    }

    /**
     * Extracts the schema version and the description from a migration name formatted as 1_2__Description.
     *
     * @param migrationName The migration name to parse. Should not contain any folders or packages.
     * @param prefix        The migration prefix.
     * @param separator     The migration separator.
     * @param suffix        The migration suffix.
     * @return The extracted schema version.
     * @throws CassandraMigrationException if the migration name does not follow the standard conventions.
     */
    public static Pair<MigrationVersion, String> extractVersionAndDescription(String migrationName,
                                                                              String prefix, String separator, String suffix) {
        String cleanMigrationName = migrationName.substring(prefix.length(), migrationName.length() - suffix.length());

        // Handle the description
        int descriptionPos = cleanMigrationName.indexOf(separator);
        if (descriptionPos < 0) {
            throw new CassandraMigrationException("Wrong migration name format: " + migrationName
                    + "(It should look like this: " + prefix + "1_2" + separator + "Description" + suffix + ")");
        }

        String version = cleanMigrationName.substring(0, descriptionPos);
        String description = cleanMigrationName.substring(descriptionPos + separator.length()).replaceAll("_", " ");
        return Pair.of(MigrationVersion.fromVersion(version), description);
    }
}
