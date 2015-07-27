package com.contrastsecurity.cassandra.migration.utils.scanner;

public interface Resource {
    /**
     * @return The location of the resource on the classpath (path and filename).
     */
    String getLocation();

    /**
     * Retrieves the location of this resource on disk.
     *
     * @return The location of this resource on disk.
     */
    String getLocationOnDisk();

    /**
     * Loads this resource as a string.
     *
     * @param encoding The encoding to use.
     * @return The string contents of the resource.
     */
    String loadAsString(String encoding);

    /**
     * Loads this resource as a byte array.
     *
     * @return The contents of the resource.
     */
    byte[] loadAsBytes();

    /**
     * @return The filename of this resource, without the path.
     */
    String getFilename();
}
