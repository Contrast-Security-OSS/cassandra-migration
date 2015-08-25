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
package com.contrastsecurity.cassandra.migration.utils.scanner.filesystem;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FileSystemResourceTest {
    @Test
    public void getFilename() throws Exception {
        assertEquals("Mig777__Test.cql", new FileSystemResource("Mig777__Test.cql").getFilename());
        assertEquals("Mig777__Test.cql", new FileSystemResource("folder/Mig777__Test.cql").getFilename());
    }

    @Test
    public void getPath() throws Exception {
        assertEquals("Mig777__Test.cql", new FileSystemResource("Mig777__Test.cql").getLocation());
        assertEquals("folder/Mig777__Test.cql", new FileSystemResource("folder/Mig777__Test.cql").getLocation());
    }
}
