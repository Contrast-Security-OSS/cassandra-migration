/**
 * Copyright 2010-2015 Axel Fontaine
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.contrastsecurity.cassandra.migration.resolver.java;

import com.contrastsecurity.cassandra.migration.CassandraMigrationException;
import com.contrastsecurity.cassandra.migration.api.MigrationChecksumProvider;
import com.contrastsecurity.cassandra.migration.api.MigrationInfoProvider;
import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.contrastsecurity.cassandra.migration.config.MigrationType;
import com.contrastsecurity.cassandra.migration.config.ScriptsLocation;
import com.contrastsecurity.cassandra.migration.info.MigrationVersion;
import com.contrastsecurity.cassandra.migration.info.ResolvedMigration;
import com.contrastsecurity.cassandra.migration.resolver.MigrationInfoHelper;
import com.contrastsecurity.cassandra.migration.resolver.MigrationResolver;
import com.contrastsecurity.cassandra.migration.resolver.ResolvedMigrationComparator;
import com.contrastsecurity.cassandra.migration.utils.ClassUtils;
import com.contrastsecurity.cassandra.migration.utils.Pair;
import com.contrastsecurity.cassandra.migration.utils.StringUtils;
import com.contrastsecurity.cassandra.migration.utils.scanner.Scanner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Migration resolver for Java migrations. The classes must have a name like V1 or V1_1_3 or V1__Description
 * or V1_1_3__Description.
 */
public class JavaMigrationResolver implements MigrationResolver {
    /**
     * The base package on the classpath where to migrations are located.
     */
    private final ScriptsLocation location;

    /**
     * The ClassLoader to use.
     */
    private ClassLoader classLoader;

    /**
     * Creates a new instance.
     *
     * @param location    The base package on the classpath where to migrations are located.
     * @param classLoader The ClassLoader for loading migrations on the classpath.
     */
    public JavaMigrationResolver(ClassLoader classLoader, ScriptsLocation location) {
        this.location = location;
        this.classLoader = classLoader;
    }

    public List<ResolvedMigration> resolveMigrations() {
        List<ResolvedMigration> migrations = new ArrayList<ResolvedMigration>();

        if (!location.isClassPath()) {
            return migrations;
        }

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

        Collections.sort(migrations, new ResolvedMigrationComparator());
        return migrations;
    }

    /**
     * Extracts the migration info from this migration.
     *
     * @param javaMigration The migration to analyse.
     * @return The migration info.
     */
    ResolvedMigration extractMigrationInfo(JavaMigration javaMigration) {
        Integer checksum = null;
        if (javaMigration instanceof MigrationChecksumProvider) {
            MigrationChecksumProvider checksumProvider = (MigrationChecksumProvider) javaMigration;
            checksum = checksumProvider.getChecksum();
        } else {
        	checksum = 0;
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
