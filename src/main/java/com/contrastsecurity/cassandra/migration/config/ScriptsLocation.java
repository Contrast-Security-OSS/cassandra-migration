package com.contrastsecurity.cassandra.migration.config;

import com.contrastsecurity.cassandra.migration.CassandraMigrationException;

public final class ScriptsLocation implements Comparable<ScriptsLocation> {

    private static final String CLASSPATH_PREFIX = "classpath:";
    public static final String FILESYSTEM_PREFIX = "filesystem:";

    private String prefix; //classpath or filesystem
    private String path;

    public ScriptsLocation(String descriptor) {
        String normalizedDescriptor = descriptor.trim().replace("\\", "/");

        if (normalizedDescriptor.contains(":")) {
            prefix = normalizedDescriptor.substring(0, normalizedDescriptor.indexOf(":") + 1);
            path = normalizedDescriptor.substring(normalizedDescriptor.indexOf(":") + 1);
        } else {
            prefix = CLASSPATH_PREFIX;
            path = normalizedDescriptor;
        }

        if (isClassPath()) {
            path = path.replace(".", "/");
            if (path.startsWith("/")) {
                path = path.substring(1);
            }
        } else {
            if (!isFileSystem()) {
                throw new CassandraMigrationException("Unknown prefix for location. " +
                        "Must be " + CLASSPATH_PREFIX + " or " + FILESYSTEM_PREFIX + "."
                        + normalizedDescriptor);
            }
        }

        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
    }

    public boolean isClassPath() {
        return CLASSPATH_PREFIX.equals(prefix);
    }

    public boolean isFileSystem() {
        return FILESYSTEM_PREFIX.equals(prefix);
    }

    public boolean isParentOf(ScriptsLocation other) {
        return (other.getDescriptor() + "/").startsWith(getDescriptor() + "/");
    }

    public String getPrefix() {
        return prefix;
    }

    public String getPath() {
        return path;
    }

    public String getDescriptor() {
        return prefix + path;
    }

    @SuppressWarnings("NullableProblems")
    public int compareTo(ScriptsLocation o) {
        return getDescriptor().compareTo(o.getDescriptor());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScriptsLocation location = (ScriptsLocation) o;

        return getDescriptor().equals(location.getDescriptor());
    }

    @Override
    public int hashCode() {
        return getDescriptor().hashCode();
    }

    @Override
    public String toString() {
        return getDescriptor();
    }
}
