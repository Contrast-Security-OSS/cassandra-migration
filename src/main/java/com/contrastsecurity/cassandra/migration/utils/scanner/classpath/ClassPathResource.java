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

import com.contrastsecurity.cassandra.migration.CassandraMigrationException;
import com.contrastsecurity.cassandra.migration.utils.FileCopyUtils;
import com.contrastsecurity.cassandra.migration.utils.scanner.Resource;

import java.io.*;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.Charset;

/**
 * A resource on the classpath.
 */
public class ClassPathResource implements Comparable<ClassPathResource>, Resource {
    /**
     * The location of the resource on the classpath.
     */
    private String location;

    /**
     * The ClassLoader to use.
     */
    private ClassLoader classLoader;

    /**
     * Creates a new ClassPathResource.
     *
     * @param location    The location of the resource on the classpath.
     * @param classLoader The ClassLoader to use.
     */
    public ClassPathResource(String location, ClassLoader classLoader) {
        this.location = location;
        this.classLoader = classLoader;
    }

    public String getLocation() {
        return location;
    }

    public String getLocationOnDisk() {
        URL url = getUrl();
        if (url == null) {
            throw new CassandraMigrationException("Unable to location resource on disk: " + location);
        }
        try {
            return new File(URLDecoder.decode(url.getPath(), "UTF-8")).getAbsolutePath();
        } catch (UnsupportedEncodingException e) {
            throw new CassandraMigrationException("Unknown encoding: UTF-8", e);
        }
    }

    /**
     * @return The url of this resource.
     */
    private URL getUrl() {
        return classLoader.getResource(location);
    }

    public String loadAsString(String encoding) {
        try {
            InputStream inputStream = classLoader.getResourceAsStream(location);
            if (inputStream == null) {
                throw new CassandraMigrationException("Unable to obtain inputstream for resource: " + location);
            }
            Reader reader = new InputStreamReader(inputStream, Charset.forName(encoding));

            return FileCopyUtils.copyToString(reader);
        } catch (IOException e) {
            throw new CassandraMigrationException("Unable to load resource: " + location + " (encoding: " + encoding + ")", e);
        }
    }

    public byte[] loadAsBytes() {
        try {
            InputStream inputStream = classLoader.getResourceAsStream(location);
            if (inputStream == null) {
                throw new CassandraMigrationException("Unable to obtain inputstream for resource: " + location);
            }
            return FileCopyUtils.copyToByteArray(inputStream);
        } catch (IOException e) {
            throw new CassandraMigrationException("Unable to load resource: " + location, e);
        }
    }

    public String getFilename() {
        return location.substring(location.lastIndexOf("/") + 1);
    }

    public boolean exists() {
        return getUrl() != null;
    }

    @SuppressWarnings({"RedundantIfStatement"})
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClassPathResource that = (ClassPathResource) o;

        if (!location.equals(that.location)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return location.hashCode();
    }

    @SuppressWarnings("NullableProblems")
    public int compareTo(ClassPathResource o) {
        return location.compareTo(o.location);
    }
}
