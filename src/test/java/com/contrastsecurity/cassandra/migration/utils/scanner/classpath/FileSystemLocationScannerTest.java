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
package com.contrastsecurity.cassandra.migration.utils.scanner.classpath;

import com.contrastsecurity.cassandra.migration.utils.UrlUtils;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Test for FileSystemClassPathLocationScanner.
 */
public class FileSystemLocationScannerTest {
    @Test
    public void findResourceNamesFromFileSystem() throws Exception {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        String path = UrlUtils.toFilePath(classLoader.getResources("migration").nextElement()) + File.separator;

        Set<String> resourceNames =
                new FileSystemClassPathLocationScanner().findResourceNamesFromFileSystem(path, "cql", new File(path, "cql"));

        assertEquals(3, resourceNames.size());
        String[] names = resourceNames.toArray(new String[3]);
        assertEquals("cql/V1_2__Populate_table.cql", names[0]);
        assertEquals("cql/V1__First.cql", names[1]);
        assertEquals("cql/V2_0__Add_contents_table.cql", names[2]);
    }

    @Test
    public void findResourceNamesNonExistantPath() throws Exception {
        URL url = new URL("file", null, 0, "X:\\dummy\\cql");

        Set<String> resourceNames =
                new FileSystemClassPathLocationScanner().findResourceNames("cql", url);

        assertEquals(0, resourceNames.size());
    }
}
