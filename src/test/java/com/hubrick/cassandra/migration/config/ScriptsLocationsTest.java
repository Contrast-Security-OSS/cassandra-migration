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

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Small Test for Locations.
 */
public class ScriptsLocationsTest {
    @Test
    public void mergeLocations() {
        ScriptsLocations locations = new ScriptsLocations("db/locations", "db/files", "db/classes");
        List<ScriptsLocation> locationList = locations.getLocations();
        assertEquals(3, locationList.size());
        Iterator<ScriptsLocation> iterator = locationList.iterator();
        assertEquals("db/classes", iterator.next().getPath());
        assertEquals("db/files", iterator.next().getPath());
        assertEquals("db/locations", iterator.next().getPath());
    }

    @Test
    public void mergeLocationsDuplicate() {
        ScriptsLocations locations = new ScriptsLocations("db/locations", "db/migration", "db/migration");
        List<ScriptsLocation> locationList = locations.getLocations();
        assertEquals(2, locationList.size());
        Iterator<ScriptsLocation> iterator = locationList.iterator();
        assertEquals("db/locations", iterator.next().getPath());
        assertEquals("db/migration", iterator.next().getPath());
    }

    @Test
    public void mergeLocationsOverlap() {
        ScriptsLocations locations = new ScriptsLocations("db/migration/oracle", "db/migration", "db/migration");
        List<ScriptsLocation> locationList = locations.getLocations();
        assertEquals(1, locationList.size());
        assertEquals("db/migration", locationList.get(0).getPath());
    }

    @Test
    public void mergeLocationsSimilarButNoOverlap() {
        ScriptsLocations locations = new ScriptsLocations("db/migration/oracle", "db/migration", "db/migrationtest");
        List<ScriptsLocation> locationList = locations.getLocations();
        assertEquals(2, locationList.size());
        assertTrue(locationList.contains(new ScriptsLocation("db/migration")));
        assertTrue(locationList.contains(new ScriptsLocation("db/migrationtest")));
    }

    @Test
    public void mergeLocationsSimilarButNoOverlapCamelCase() {
        ScriptsLocations locations = new ScriptsLocations("/com/xxx/Star/", "/com/xxx/StarTrack/");
        List<ScriptsLocation> locationList = locations.getLocations();
        assertEquals(2, locationList.size());
        assertTrue(locationList.contains(new ScriptsLocation("com/xxx/Star")));
        assertTrue(locationList.contains(new ScriptsLocation("com/xxx/StarTrack")));
    }

    @Test
    public void mergeLocationsSimilarButNoOverlapHyphen() {
        ScriptsLocations locations = new ScriptsLocations("db/migration/oracle", "db/migration", "db/migration-test");
        List<ScriptsLocation> locationList = locations.getLocations();
        assertEquals(2, locationList.size());
        assertTrue(locationList.contains(new ScriptsLocation("db/migration")));
        assertTrue(locationList.contains(new ScriptsLocation("db/migration-test")));
    }
}
