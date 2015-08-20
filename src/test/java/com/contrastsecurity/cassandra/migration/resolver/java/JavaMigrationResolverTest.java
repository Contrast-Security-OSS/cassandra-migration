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
import com.contrastsecurity.cassandra.migration.config.ScriptsLocation;
import com.contrastsecurity.cassandra.migration.info.ResolvedMigration;
import com.contrastsecurity.cassandra.migration.resolver.java.dummy.V2__InterfaceBasedMigration;
import com.contrastsecurity.cassandra.migration.resolver.java.dummy.Version3dot5;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Test for JavaMigrationResolver.
 */
public class JavaMigrationResolverTest {
    @Test(expected = CassandraMigrationException.class)
    public void broken() {
        new JavaMigrationResolver(Thread.currentThread().getContextClassLoader(), new ScriptsLocation("com/contrastsecurity/cassandra/migration/resolver/java/error")).resolveMigrations();
    }

    @Test
    public void resolveMigrations() {
        JavaMigrationResolver jdbcMigrationResolver =
                new JavaMigrationResolver(Thread.currentThread().getContextClassLoader(), new ScriptsLocation("com/contrastsecurity/cassandra/migration/resolver/java/dummy"));
        Collection<ResolvedMigration> migrations = jdbcMigrationResolver.resolveMigrations();

        assertEquals(3, migrations.size());

        List<ResolvedMigration> migrationList = new ArrayList<ResolvedMigration>(migrations);

        ResolvedMigration migrationInfo = migrationList.get(0);
        assertEquals("2", migrationInfo.getVersion().toString());
        assertEquals("InterfaceBasedMigration", migrationInfo.getDescription());
        assertNull(migrationInfo.getChecksum());

        ResolvedMigration migrationInfo1 = migrationList.get(1);
        assertEquals("3.5", migrationInfo1.getVersion().toString());
        assertEquals("Three Dot Five", migrationInfo1.getDescription());
        assertEquals(35, migrationInfo1.getChecksum().intValue());

        ResolvedMigration migrationInfo2 = migrationList.get(2);
        assertEquals("4", migrationInfo2.getVersion().toString());
    }

    @Test
    public void conventionOverConfiguration() {
        JavaMigrationResolver jdbcMigrationResolver = new JavaMigrationResolver(Thread.currentThread().getContextClassLoader(), null);
        ResolvedMigration migrationInfo = jdbcMigrationResolver.extractMigrationInfo(new V2__InterfaceBasedMigration());
        assertEquals("2", migrationInfo.getVersion().toString());
        assertEquals("InterfaceBasedMigration", migrationInfo.getDescription());
        assertNull(migrationInfo.getChecksum());
    }

    @Test
    public void explicitInfo() {
        JavaMigrationResolver jdbcMigrationResolver = new JavaMigrationResolver(Thread.currentThread().getContextClassLoader(), null);
        ResolvedMigration migrationInfo = jdbcMigrationResolver.extractMigrationInfo(new Version3dot5());
        assertEquals("3.5", migrationInfo.getVersion().toString());
        assertEquals("Three Dot Five", migrationInfo.getDescription());
        assertEquals(35, migrationInfo.getChecksum().intValue());
    }
}
