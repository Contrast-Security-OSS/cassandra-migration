package com.contrastsecurity.cassandra.migration.config;

import com.contrastsecurity.cassandra.migration.info.MigrationVersion;
import com.contrastsecurity.cassandra.migration.utils.StringUtils;

public class MigrationConfigs {
    public enum MigrationProperty {
        SCRIPTS_ENCODING("cassandra.migration.scripts.encoding", "Encoding for CQL scripts"),
        SCRIPTS_LOCATIONS("cassandra.migration.scripts.locations", "Locations of the migration scripts in CSV format"),
        ALLOW_OUTOFORDER("cassandra.migration.scripts.allowoutoforder", "Allow out of order migration"),
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
        if (null != scriptsEncodingP && scriptsEncodingP.trim().length() != 0)
            this.encoding = scriptsEncodingP;

        String targetVersionP = System.getProperty(MigrationProperty.TARGET_VERSION.getName());
        if (null != targetVersionP && targetVersionP.trim().length() != 0)
            setTargetAsString(targetVersionP);

        String locationsProp = System.getProperty(MigrationProperty.SCRIPTS_LOCATIONS.getName());
        if (locationsProp != null && locationsProp.trim().length() != 0) {
            scriptsLocations = StringUtils.tokenizeToStringArray(locationsProp, ",");
        }

        String allowOutOfOrderProp = System.getProperty(MigrationProperty.ALLOW_OUTOFORDER.getName());
        if(allowOutOfOrderProp != null && allowOutOfOrderProp.trim().length() != 0) {
            setAllowOutOfOrder(allowOutOfOrderProp);
        }
    }

    /**
     * The encoding of Cql migration scripts (default: UTF-8)
     */
    private String encoding = "UTF-8";

    /**
     * Locations of the migration scripts in CSV format (default: db/migration)
     */
    private String[] scriptsLocations = {"db/migration"};

    /**
     * Allow out of order migrations (default: false)
     */
    private boolean allowOutOfOrder = false;

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

    public String[] getScriptsLocations() {
        return scriptsLocations;
    }

    public void setScriptsLocations(String[] scriptsLocations) {
        this.scriptsLocations = scriptsLocations;
    }

    public boolean isAllowOutOfOrder() {
        return allowOutOfOrder;
    }

    public void setAllowOutOfOrder(String allowOutOfOrder) {
        this.allowOutOfOrder = Boolean.parseBoolean(allowOutOfOrder);
    }

    public void setAllowOutOfOrder(boolean allowOutOfOrder) {
        this.allowOutOfOrder = allowOutOfOrder;
    }

    public MigrationVersion getTarget() {
        return target;
    }

    /**
     * Migrations with a higher version number will be ignored. (default: the latest version)
     * @param target Target version
     */
    public void setTargetAsString(String target) {
        this.target = MigrationVersion.fromVersion(target);
    }
}
