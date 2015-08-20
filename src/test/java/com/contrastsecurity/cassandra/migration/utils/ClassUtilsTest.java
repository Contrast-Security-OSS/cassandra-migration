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
package com.contrastsecurity.cassandra.migration.utils;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for ClassUtils.
 */
public class ClassUtilsTest {
    @Test
    public void isPresent() {
        assertTrue(ClassUtils.isPresent("com.contrastsecurity.cassandra.migration.CassandraMigration", Thread.currentThread().getContextClassLoader()));
    }

    @Test
    public void isPresentNot() {
        assertFalse(ClassUtils.isPresent("com.example.FakeClass", Thread.currentThread().getContextClassLoader()));
    }
}
