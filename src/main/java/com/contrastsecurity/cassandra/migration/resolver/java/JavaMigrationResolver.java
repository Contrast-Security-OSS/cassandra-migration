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

import com.contrastsecurity.cassandra.migration.config.ScriptsLocation;
import com.contrastsecurity.cassandra.migration.info.ResolvedMigration;
import com.contrastsecurity.cassandra.migration.resolver.MigrationResolver;
import com.contrastsecurity.cassandra.migration.resolver.ResolvedMigrationComparator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Migration resolver for Java migrations. The classes must have a name like V1 or V1_1_3 or V1__Description
 * or V1_1_3__Description.
 */
public class JavaMigrationResolver extends CommonJavaResolver implements MigrationResolver {
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
        if (!location.isClassPath()) {
            return Collections.emptyList();
        }

        List<ResolvedMigration> migrations = loadJavaMigrationFiles(classLoader, location);

        Collections.sort(migrations, new ResolvedMigrationComparator());
        return migrations;
    }


}
