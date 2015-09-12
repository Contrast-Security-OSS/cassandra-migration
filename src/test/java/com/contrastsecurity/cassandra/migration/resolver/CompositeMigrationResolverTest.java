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
package com.contrastsecurity.cassandra.migration.resolver;

import com.contrastsecurity.cassandra.migration.CassandraMigrationException;
import com.contrastsecurity.cassandra.migration.config.MigrationType;
import com.contrastsecurity.cassandra.migration.config.ScriptsLocations;
import com.contrastsecurity.cassandra.migration.info.MigrationVersion;
import com.contrastsecurity.cassandra.migration.info.ResolvedMigration;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for CompositeMigrationResolver.
 */
public class CompositeMigrationResolverTest {
    @Test
    public void resolveMigrationsMultipleLocations() {
        MigrationResolver migrationResolver = new CompositeMigrationResolver(
                Thread.currentThread().getContextClassLoader(),
                new ScriptsLocations("migration/subdir/dir2", "migration.outoforder", "migration/subdir/dir1"),
                "UTF-8");

        Collection<ResolvedMigration> migrations = migrationResolver.resolveMigrations();
        List<ResolvedMigration> migrationList = new ArrayList<ResolvedMigration>(migrations);

        assertEquals(3, migrations.size());
        assertEquals("First", migrationList.get(0).getDescription());
        assertEquals("Late arrival", migrationList.get(1).getDescription());
        assertEquals("Add contents table", migrationList.get(2).getDescription());
    }

    /**
     * Checks that migrations are properly collected, eliminating all exact duplicates.
     */
    @Test
    public void collectMigrations() {
        MigrationResolver migrationResolver = new MigrationResolver() {
            public List<ResolvedMigration> resolveMigrations() {
                List<ResolvedMigration> migrations = new ArrayList<ResolvedMigration>();

                migrations.add(createTestMigration(MigrationType.JAVA_DRIVER, "1", "Description", "Migration1", 123));
                migrations.add(createTestMigration(MigrationType.JAVA_DRIVER, "1", "Description", "Migration1", 123));
                migrations.add(createTestMigration(MigrationType.CQL, "2", "Description2", "Migration2", 1234));
                return migrations;
            }
        };
        Collection<MigrationResolver> migrationResolvers = new ArrayList<MigrationResolver>();
        migrationResolvers.add(migrationResolver);

        Collection<ResolvedMigration> migrations = CompositeMigrationResolver.collectMigrations(migrationResolvers);
        assertEquals(2, migrations.size());
    }

    @Test
    public void checkForIncompatibilitiesMessage() {
        ResolvedMigration migration1 = createTestMigration(MigrationType.CQL, "1", "First", "V1__First.cql", 123);
        migration1.setPhysicalLocation("target/test-classes/migration/validate/V1__First.cql");

        ResolvedMigration migration2 = createTestMigration(MigrationType.JAVA_DRIVER, "1", "Description", "Migration1", 123);
        migration2.setPhysicalLocation("Migration1");

        List<ResolvedMigration> migrations = new ArrayList<ResolvedMigration>();
        migrations.add(migration1);
        migrations.add(migration2);

        try {
            CompositeMigrationResolver.checkForIncompatibilities(migrations);
        } catch (CassandraMigrationException e) {
            assertTrue(e.getMessage().contains("target/test-classes/migration/validate/V1__First.cql"));
            assertTrue(e.getMessage().contains("Migration1"));
        }
    }

    /**
     * Makes sure no validation exception is thrown.
     */
    @Test
    public void checkForIncompatibilitiesNoConflict() {
        List<ResolvedMigration> migrations = new ArrayList<ResolvedMigration>();
        migrations.add(createTestMigration(MigrationType.JAVA_DRIVER, "1", "Description", "Migration1", 123));
        migrations.add(createTestMigration(MigrationType.CQL, "2", "Description2", "Migration2", 1234));

        CompositeMigrationResolver.checkForIncompatibilities(migrations);
    }

    /**
     * Creates a migration for our tests.
     *
     * @param aMigrationType The migration type.
     * @param aVersion       The version.
     * @param aDescription   The description.
     * @param aScript        The script.
     * @param aChecksum      The checksum.
     * @return The new test migration.
     */
    private ResolvedMigration createTestMigration(final MigrationType aMigrationType, final String aVersion, final String aDescription, final String aScript, final Integer aChecksum) {
        ResolvedMigration migration = new ResolvedMigration();
        migration.setVersion(MigrationVersion.fromVersion(aVersion));
        migration.setDescription(aDescription);
        migration.setScript(aScript);
        migration.setChecksum(aChecksum);
        migration.setType(aMigrationType);
        return migration;
    }

}
