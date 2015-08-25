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
import com.contrastsecurity.cassandra.migration.info.MigrationVersion;
import com.contrastsecurity.cassandra.migration.utils.Pair;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test for MigrationInfoHelper.
 */
public class MigrationInfoHelperTest {
    /**
     * Tests a schema version that lacks a description.
     */
    @Test(expected = CassandraMigrationException.class)
    public void extractSchemaVersionNoDescription() {
        MigrationInfoHelper.extractVersionAndDescription("9_4", "", "__", "");
    }

    @Test
    public void extractSchemaVersionDefaults() {
        Pair<MigrationVersion, String> info = MigrationInfoHelper.extractVersionAndDescription("V9_4__EmailAxel.cql", "V", "__", ".cql");
        MigrationVersion version = info.getLeft();
        String description = info.getRight();
        assertEquals("9.4", version.toString());
        assertEquals("EmailAxel", description);
    }

    @Test
    public void extractSchemaVersionCustomSeparator() {
        Pair<MigrationVersion, String> info = MigrationInfoHelper.extractVersionAndDescription("V9_4-EmailAxel.cql", "V", "-", ".cql");
        MigrationVersion version = info.getLeft();
        String description = info.getRight();
        assertEquals("9.4", version.toString());
        assertEquals("EmailAxel", description);
    }

    /**
     * Tests a schema version that includes a description.
     */
    @Test
    public void extractSchemaVersionWithDescription() {
        Pair<MigrationVersion, String> info = MigrationInfoHelper.extractVersionAndDescription("9_4__EmailAxel", "", "__", "");
        MigrationVersion version = info.getLeft();
        String description = info.getRight();
        assertEquals("9.4", version.toString());
        assertEquals("EmailAxel", description);
    }

    /**
     * Tests a schema version that includes a description with spaces.
     */
    @Test
    public void extractSchemaVersionWithDescriptionWithSpaces() {
        Pair<MigrationVersion, String> info = MigrationInfoHelper.extractVersionAndDescription("9_4__Big_jump", "", "__", "");
        MigrationVersion version = info.getLeft();
        String description = info.getRight();
        assertEquals("9.4", version.toString());
        assertEquals("Big jump", description);
    }

    /**
     * Tests a schema version that includes a version with leading zeroes.
     */
    @Test
    public void extractSchemaVersionWithLeadingZeroes() {
        Pair<MigrationVersion, String> info = MigrationInfoHelper.extractVersionAndDescription("009_4__EmailAxel", "", "__", "");
        MigrationVersion version = info.getLeft();
        String description = info.getRight();
        assertEquals("009.4", version.toString());
        assertEquals("EmailAxel", description);
    }

    @Test(expected = CassandraMigrationException.class)
    public void extractSchemaVersionWithLeadingUnderscore() {
        MigrationInfoHelper.extractVersionAndDescription("_8_0__Description", "", "__", "");
    }

    @Test(expected = CassandraMigrationException.class)
    public void extractSchemaVersionWithLeadingUnderscoreAndPrefix() {
        MigrationInfoHelper.extractVersionAndDescription("V_8_0__Description.cql", "V", "__", ".cql");
    }

    @Test
    public void extractSchemaVersionWithVUnderscorePrefix() {
        Pair<MigrationVersion, String> info = MigrationInfoHelper.extractVersionAndDescription("V_8_0__Description.cql", "V_", "__", ".cql");
        MigrationVersion version = info.getLeft();
        String description = info.getRight();
        assertEquals("8.0", version.toString());
        assertEquals("Description", description);
    }
}
