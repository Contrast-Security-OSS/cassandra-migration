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
package com.hubrick.cassandra.migration.config;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test for location.
 */
public class ScriptsLocationTest {
    @Test
    public void defaultPrefix() {
        ScriptsLocation location = new ScriptsLocation("db/migration");
        assertEquals("classpath:", location.getPrefix());
        assertTrue(location.isClassPath());
        assertEquals("db/migration", location.getPath());
        assertEquals("classpath:db/migration", location.getDescriptor());
    }

    @Test
    public void classpathPrefix() {
        ScriptsLocation location = new ScriptsLocation("classpath:db/migration");
        assertEquals("classpath:", location.getPrefix());
        assertTrue(location.isClassPath());
        assertEquals("db/migration", location.getPath());
        assertEquals("classpath:db/migration", location.getDescriptor());
    }

    @Test
    public void filesystemPrefix() {
        ScriptsLocation location = new ScriptsLocation("filesystem:db/migration");
        assertEquals("filesystem:", location.getPrefix());
        assertFalse(location.isClassPath());
        assertEquals("db/migration", location.getPath());
        assertEquals("filesystem:db/migration", location.getDescriptor());
    }

    @Test
    public void filesystemPrefixAbsolutePath() {
        ScriptsLocation location = new ScriptsLocation("filesystem:/db/migration");
        assertEquals("filesystem:", location.getPrefix());
        assertFalse(location.isClassPath());
        assertEquals("/db/migration", location.getPath());
        assertEquals("filesystem:/db/migration", location.getDescriptor());
    }

    @Test
    public void filesystemPrefixWithDotsInPath() {
        ScriptsLocation location = new ScriptsLocation("filesystem:util-2.0.4/db/migration");
        assertEquals("filesystem:", location.getPrefix());
        assertFalse(location.isClassPath());
        assertEquals("util-2.0.4/db/migration", location.getPath());
        assertEquals("filesystem:util-2.0.4/db/migration", location.getDescriptor());
    }
}
