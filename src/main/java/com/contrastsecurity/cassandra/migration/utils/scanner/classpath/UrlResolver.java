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

import java.io.IOException;
import java.net.URL;

/**
 * Resolves container-specific URLs into standard Java URLs.
 */
public interface UrlResolver {
    /**
     * Resolves this container-specific URL into standard Java URL.
     *
     * @param url The URL to resolve.
     * @return The matching standard Java URL.
     * @throws IOException when the scanning failed.
     */
    URL toStandardJavaUrl(URL url) throws IOException;
}
