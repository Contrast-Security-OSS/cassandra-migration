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
package com.contrastsecurity.cassandra.migration.info;

import com.contrastsecurity.cassandra.migration.config.MigrationType;
import com.contrastsecurity.cassandra.migration.dao.SchemaVersionDAO;
import com.contrastsecurity.cassandra.migration.resolver.MigrationResolver;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for MigrationInfoServiceImpl.
 */
public class MigrationInfoServiceTest {
    @Test
    public void onlyPending() {
        MigrationInfoService migrationInfoService =
                new MigrationInfoService(
                        createMigrationResolver(createAvailableMigration(1), createAvailableMigration(2)),
                        createSchemaVersionDAO(), MigrationVersion.LATEST, false, true);
        migrationInfoService.refresh();

        assertNull(migrationInfoService.current());
        assertEquals(2, migrationInfoService.all().length);
        assertEquals(2, migrationInfoService.pending().length);
    }

    @Test
    public void allApplied() {
        MigrationInfoService migrationInfoService =
                new MigrationInfoService(
                        createMigrationResolver(createAvailableMigration(1), createAvailableMigration(2)),
                        createSchemaVersionDAO(createAppliedMigration(1), createAppliedMigration(2)),
                        MigrationVersion.LATEST, false, true);
        migrationInfoService.refresh();

        assertEquals("2", migrationInfoService.current().getVersion().toString());
        assertEquals(2, migrationInfoService.all().length);
        assertEquals(0, migrationInfoService.pending().length);
    }

    @Test
    public void appliedOverridesAvailable() {
        MigrationInfoService migrationInfoService =
                new MigrationInfoService(
                        createMigrationResolver(createAvailableMigration(1)),
                        createSchemaVersionDAO(createAppliedMigration(1, "xyz")),
                        MigrationVersion.LATEST, false, true);
        migrationInfoService.refresh();

        assertEquals("1", migrationInfoService.current().getVersion().toString());
        assertEquals("xyz", migrationInfoService.current().getDescription());
        assertEquals(1, migrationInfoService.all().length);
        assertEquals(0, migrationInfoService.pending().length);
    }

    @Test
    public void onePendingOneApplied() {
        MigrationInfoService migrationInfoService =
                new MigrationInfoService(
                        createMigrationResolver(createAvailableMigration(1), createAvailableMigration(2)),
                        createSchemaVersionDAO(createAppliedMigration(1)),
                        MigrationVersion.LATEST, false, true);
        migrationInfoService.refresh();

        assertEquals("1", migrationInfoService.current().getVersion().toString());
        assertEquals(2, migrationInfoService.all().length);
        assertEquals(1, migrationInfoService.pending().length);
    }

    @Test
    public void oneAppliedOneSkipped() {
        MigrationInfoService migrationInfoService =
                new MigrationInfoService(
                        createMigrationResolver(createAvailableMigration(1), createAvailableMigration(2)),
                        createSchemaVersionDAO(createAppliedMigration(2)),
                        MigrationVersion.LATEST, false, true);
        migrationInfoService.refresh();

        assertEquals("2", migrationInfoService.current().getVersion().toString());
        assertEquals(MigrationState.IGNORED, migrationInfoService.all()[0].getState());
        assertEquals(2, migrationInfoService.all().length);
        assertEquals(0, migrationInfoService.pending().length);
    }

    @Test
    public void twoAppliedOneFuture() {
        MigrationInfoService migrationInfoService =
                new MigrationInfoService(
                        createMigrationResolver(createAvailableMigration(1)),
                        createSchemaVersionDAO(createAppliedMigration(1), createAppliedMigration(2)),
                        MigrationVersion.LATEST, false, true);
        migrationInfoService.refresh();

        assertEquals("2", migrationInfoService.current().getVersion().toString());
        assertEquals(MigrationState.FUTURE_SUCCESS, migrationInfoService.current().getState());
        assertEquals(MigrationState.FUTURE_SUCCESS, migrationInfoService.future()[0].getState());
        assertEquals(2, migrationInfoService.all().length);
        assertEquals(0, migrationInfoService.pending().length);
    }

    @Test
    public void belowBaseline() {
        MigrationInfoService migrationInfoService =
                new MigrationInfoService(
                        createMigrationResolver(createAvailableMigration(1)),
                        createSchemaVersionDAO(createAppliedInitMigration(2)),
                        MigrationVersion.LATEST, false, true);
        migrationInfoService.refresh();

        assertEquals("2", migrationInfoService.current().getVersion().toString());
        assertEquals(MigrationState.BELOW_BASELINE, migrationInfoService.all()[0].getState());
        assertEquals(2, migrationInfoService.all().length);
        assertEquals(0, migrationInfoService.pending().length);
    }

    @Test
    public void missing() {
        MigrationInfoService migrationInfoService =
                new MigrationInfoService(
                        createMigrationResolver(createAvailableMigration(2)),
                        createSchemaVersionDAO(createAppliedMigration(1), createAppliedMigration(2)),
                        MigrationVersion.LATEST, false, true);
        migrationInfoService.refresh();

        assertEquals("2", migrationInfoService.current().getVersion().toString());
        assertEquals(MigrationState.MISSING_SUCCESS, migrationInfoService.all()[0].getState());
        assertEquals(2, migrationInfoService.all().length);
        assertEquals(0, migrationInfoService.pending().length);
    }

    @Test
    public void schemaCreation() {
        MigrationInfoService migrationInfoService =
                new MigrationInfoService(
                        createMigrationResolver(createAvailableMigration(1)),
                        createSchemaVersionDAO(createAppliedSchemaMigration(), createAppliedMigration(1)),
                        MigrationVersion.LATEST, false, true);
        migrationInfoService.refresh();

        assertEquals("1", migrationInfoService.current().getVersion().toString());
        assertEquals(MigrationState.SUCCESS, migrationInfoService.all()[0].getState());
        assertEquals(MigrationState.SUCCESS, migrationInfoService.all()[1].getState());
        assertEquals(2, migrationInfoService.all().length);
        assertEquals(0, migrationInfoService.pending().length);
    }

    /**
     * Creates a new available migration with this version.
     *
     * @param version The version of the migration.
     * @return The available migration.
     */
    private ResolvedMigration createAvailableMigration(int version) {
        ResolvedMigration migration = new ResolvedMigration();
        migration.setVersion(MigrationVersion.fromVersion(Integer.toString(version)));
        migration.setDescription("abc");
        migration.setScript("x");
        migration.setType(MigrationType.CQL);
        return migration;
    }

    /**
     * Creates a new applied migration with this version.
     *
     * @param version The version of the migration.
     * @return The applied migration.
     */
    private AppliedMigration createAppliedMigration(int version) {
        return createAppliedMigration(version, "x");
    }

    /**
     * Creates a new applied migration with this version.
     *
     * @param version     The version of the migration.
     * @param description The description of the migration.
     * @return The applied migration.
     */
    private AppliedMigration createAppliedMigration(int version, String description) {
        return new AppliedMigration(version, version, MigrationVersion.fromVersion(Integer.toString(version)), description,
                MigrationType.CQL, "x", null, new Date(), "sa", 123, true);
    }

    /**
     * Creates a new applied baseline migration with this version.
     *
     * @param version The version of the migration.
     * @return The applied baseline migration.
     */
    private AppliedMigration createAppliedInitMigration(int version) {
        return new AppliedMigration(version, version, MigrationVersion.fromVersion(Integer.toString(version)), "abc",
                MigrationType.BASELINE, "x", null, new Date(), "sa", 0, true);
    }

    /**
     * Creates a new applied schema migration with this version.
     *
     * @return The applied schema migration.
     */
    private AppliedMigration createAppliedSchemaMigration() {
        return new AppliedMigration(0, 0, MigrationVersion.fromVersion(Integer.toString(0)), "<< Schema Creation >>",
                MigrationType.SCHEMA, "x", null, new Date(), "sa", 0, true);
    }

    /**
     * Creates a migrationResolver for testing.
     *
     * @param resolvedMigrations The resolved migrations.
     * @return The migration resolver.
     */
    private MigrationResolver createMigrationResolver(final ResolvedMigration... resolvedMigrations) {
        return new MigrationResolver() {
            public List<ResolvedMigration> resolveMigrations() {
                return Arrays.asList(resolvedMigrations);
            }
        };
    }

    /**
     * Creates a metadata table for testing.
     *
     * @param appliedMigrations The applied migrations.
     * @return The metadata table.
     */
    private SchemaVersionDAO createSchemaVersionDAO(final AppliedMigration... appliedMigrations) {
        SchemaVersionDAO dao = mock(SchemaVersionDAO.class);
        when(dao.findAppliedMigrations()).thenReturn(Arrays.asList(appliedMigrations));
        return dao;
    }
}
