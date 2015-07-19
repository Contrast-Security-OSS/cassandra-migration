package com.contrastsecurity.cassandra.migration;

public class Scripts {
    private static final String PROPERTY_PREFIX = "cassandra.migration.scripts.";

    public enum ScriptsProperty {
        LOCATION(PROPERTY_PREFIX + "location", "Location of the migration scripts");

        private String name;
        private String description;

        ScriptsProperty(String name, String description) {
            this.name = name;
            this.description = description;
        }

        public String getName() {
            return name;
        }

        public String getDescription() {
            return description;
        }
    }

    private String repository;

}
