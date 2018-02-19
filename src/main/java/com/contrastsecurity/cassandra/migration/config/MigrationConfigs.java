package com.contrastsecurity.cassandra.migration.config;

import com.contrastsecurity.cassandra.migration.info.MigrationVersion;
import com.contrastsecurity.cassandra.migration.utils.StringUtils;

public class MigrationConfigs {
    final static String PREFIX = "cassandra.migration.";
    final static String ENV_PREFIX = "CASSANDRA_MIGRATION_";
    public enum MigrationProperty {
        SCRIPTS_ENCODING(PREFIX + "scripts.encoding", ENV_PREFIX + "SCRIPTS_ENCODING","Encoding for CQL scripts"),
        SCRIPTS_LOCATIONS(PREFIX + "scripts.locations", ENV_PREFIX + "SCRIPTS_LOCATIONS", "Locations of the migration scripts in CSV format"),
        ALLOW_OUTOFORDER(PREFIX + "scripts.allowoutoforder", ENV_PREFIX + "ALLOWOUTOFORDER", "Allow out of order migration"),
        TARGET_VERSION(PREFIX + "version.target", ENV_PREFIX + "VERSION_TARGET", "The target version. Migrations with a higher version number will be ignored.");

        private String name;
        private String envName;
        private String description;

        MigrationProperty(String name, String envName, String description) {
            this.name = name;
            this.envName = envName;
            this.description = description;
        }

        public String getName() {
            return name;
        }

        public String getEnvName() {
            return envName;
        }

        public String getDescription() {
            return description;
        }
    }

    public MigrationConfigs() {
        String scriptsEncodingP = PropertyGetter.getProperty(MigrationProperty.SCRIPTS_ENCODING.getName(), MigrationProperty.SCRIPTS_ENCODING.getEnvName());
        if (null != scriptsEncodingP && scriptsEncodingP.trim().length() != 0)
            this.encoding = scriptsEncodingP;

        String targetVersionP = PropertyGetter.getProperty(MigrationProperty.TARGET_VERSION.getName(), MigrationProperty.TARGET_VERSION.getEnvName());
        if (null != targetVersionP && targetVersionP.trim().length() != 0)
            setTargetAsString(targetVersionP);

        String locationsProp = PropertyGetter.getProperty(MigrationProperty.SCRIPTS_LOCATIONS.getName(), MigrationProperty.SCRIPTS_LOCATIONS.getEnvName());
        if (locationsProp != null && locationsProp.trim().length() != 0) {
            scriptsLocations = StringUtils.tokenizeToStringArray(locationsProp, ",");
        }

        String allowOutOfOrderProp = PropertyGetter.getProperty(MigrationProperty.ALLOW_OUTOFORDER.getName(), MigrationProperty.ALLOW_OUTOFORDER.getEnvName());
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
