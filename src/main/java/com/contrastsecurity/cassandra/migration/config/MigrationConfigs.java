package com.contrastsecurity.cassandra.migration.config;

import com.contrastsecurity.cassandra.migration.info.MigrationVersion;

public class MigrationConfigs {
    public enum MigrationProperty {
        SCRIPTS_ENCODING("cassandra.migration.scripts.encoding", "Encoding for CQL scripts"),
        TARGET_VERSION("cassandra.migration.version.target", "The target version. Migrations with a higher version number will be ignored.");

        private String name;
        private String description;

        MigrationProperty(String name, String description) {
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

    public MigrationConfigs() {
        String scriptsEncodingP = System.getProperty(MigrationProperty.SCRIPTS_ENCODING.getName());
        if(null != scriptsEncodingP && scriptsEncodingP.trim().length() != 0)
            this.encoding = scriptsEncodingP;

        String targetVersionP = System.getProperty(MigrationProperty.TARGET_VERSION.getName());
        if(null != targetVersionP && targetVersionP.trim().length() !=0)
            setTargetAsString(targetVersionP);
    }

    /**
     * The encoding of Cql migration scripts (default: UTF-8)
     */
    private String encoding = "UTF-8";

    /**
     * The target version. Migrations with a higher version number will be ignored. (default: the latest version)
     */
    private MigrationVersion target = MigrationVersion.LATEST;

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public MigrationVersion getTarget() {
        return target;
    }

    /**
     * Migrations with a higher version number will be ignored. (default: the latest version)
     */
    public void setTargetAsString(String target) {
        this.target = MigrationVersion.fromVersion(target);
    }
}
