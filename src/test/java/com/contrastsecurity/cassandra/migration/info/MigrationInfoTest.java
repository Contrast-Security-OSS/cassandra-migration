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
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class MigrationInfoTest {
    @Test
    public void validate() {
        MigrationVersion version = MigrationVersion.fromVersion("1");
        String description = "test";
        String user = "testUser";
        MigrationType type = MigrationType.CQL;

        ResolvedMigration resolvedMigration = new ResolvedMigration();
        resolvedMigration.setVersion(version);
        resolvedMigration.setDescription(description);
        resolvedMigration.setType(type);
        resolvedMigration.setChecksum(456);

        AppliedMigration appliedMigration = new AppliedMigration(version, description, type, null, 123, user, 0, true);

        MigrationInfo migrationInfo =
                new MigrationInfo(resolvedMigration, appliedMigration, new MigrationInfoContext());
        String message = migrationInfo.validate();

        assertTrue(message.contains("123"));
        assertTrue(message.contains("456"));
    }
}
