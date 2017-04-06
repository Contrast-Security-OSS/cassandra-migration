package com.contrastsecurity.cassandra.migration.resolver.java;

import com.contrastsecurity.cassandra.migration.CassandraMigrationException;
import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.contrastsecurity.cassandra.migration.api.MigrationChecksumProvider;
import com.contrastsecurity.cassandra.migration.api.MigrationInfoProvider;
import com.contrastsecurity.cassandra.migration.config.MigrationType;
import com.contrastsecurity.cassandra.migration.config.ScriptsLocation;
import com.contrastsecurity.cassandra.migration.info.MigrationVersion;
import com.contrastsecurity.cassandra.migration.info.ResolvedMigration;
import com.contrastsecurity.cassandra.migration.resolver.MigrationInfoHelper;
import com.contrastsecurity.cassandra.migration.utils.ClassUtils;
import com.contrastsecurity.cassandra.migration.utils.Pair;
import com.contrastsecurity.cassandra.migration.utils.StringUtils;
import com.contrastsecurity.cassandra.migration.utils.scanner.Scanner;

import java.util.List;

/**
 * Created by Adam Król on 06.04.2017.
 *
 * @author Adam Król
 */
public class CommonJavaResolver {

    public void loadJavaMigrationFiles(ClassLoader classLoader, ScriptsLocation location, List<ResolvedMigration> migrations) {
        try {
            Class<?>[] classes = new Scanner(classLoader).scanForClasses(location, JavaMigration.class);
            for (Class<?> clazz : classes) {
                JavaMigration javaMigration = ClassUtils.instantiate(clazz.getName(), classLoader);

                ResolvedMigration migrationInfo = extractMigrationInfo(javaMigration);
                migrationInfo.setPhysicalLocation(ClassUtils.getLocationOnDisk(clazz));
                migrationInfo.setExecutor(new JavaMigrationExecutor(javaMigration));

                migrations.add(migrationInfo);
            }
        } catch (Exception e) {
            throw new CassandraMigrationException("Unable to resolve Java migrations in location: " + location, e);
        }
    }

    /**
     * Extracts the migration info from this migration.
     *
     * @param javaMigration The migration to analyse.
     * @return The migration info.
     */
    public ResolvedMigration extractMigrationInfo(JavaMigration javaMigration) {
        Integer checksum = null;
        if (javaMigration instanceof MigrationChecksumProvider) {
            MigrationChecksumProvider checksumProvider = (MigrationChecksumProvider) javaMigration;
            checksum = checksumProvider.getChecksum();
        }

        MigrationVersion version;
        String description;
        if (javaMigration instanceof MigrationInfoProvider) {
            MigrationInfoProvider infoProvider = (MigrationInfoProvider) javaMigration;
            version = infoProvider.getVersion();
            description = infoProvider.getDescription();
            if (!StringUtils.hasText(description)) {
                throw new CassandraMigrationException("Missing description for migration " + version);
            }
        } else {
            Pair<MigrationVersion, String> info =
                    MigrationInfoHelper.extractVersionAndDescription(
                            ClassUtils.getShortName(javaMigration.getClass()), "V", "__", "");
            version = info.getLeft();
            description = info.getRight();
        }

        String script = javaMigration.getClass().getName();


        ResolvedMigration resolvedMigration = new ResolvedMigration();
        resolvedMigration.setVersion(version);
        resolvedMigration.setDescription(description);
        resolvedMigration.setScript(script);
        resolvedMigration.setChecksum(checksum);
        resolvedMigration.setType(MigrationType.JAVA_DRIVER);
        return resolvedMigration;
    }
}
