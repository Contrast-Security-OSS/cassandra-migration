package com.contrastsecurity.cassandra.migration;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class MigrationVersion {
    private static final String PROPERTY_PREFIX = "cassandra.migration.version.";

    public enum MigrationVersionProperty {

        TABLE(PROPERTY_PREFIX + "table", "Migration version table name");

        private String name;
        private String description;

        MigrationVersionProperty(String name, String description) {
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

    private String table = "migration_version";
    private List<BigInteger> versionParts;
    private String displayText;

    private static Pattern splitPattern = Pattern.compile("\\.(?=\\d)");

    public MigrationVersion() {
        String tableP = System.getProperty(MigrationVersionProperty.TABLE.getName());
        if(null != tableP)
            this.table = tableP;
    }

    public String getTable() {
        return table;
    }

    private void setVersion(String version) {
        String normalizedVersion = version.replace('_', '.');
        this.versionParts = tokenize(normalizedVersion);
        this.displayText = normalizedVersion;
    }

    private List<BigInteger> tokenize(String str) {
        List<BigInteger> numbers = new ArrayList<>();
        for (String number : splitPattern.split(str)) {
            try {
                numbers.add(new BigInteger(number));
            } catch (NumberFormatException e) {
                throw new CassandraMigrationException(
                        "Invalid version containing non-numeric characters. Only 0..9 and . are allowed. Invalid version: "
                                + str);
            }
        }
        for (int i = numbers.size() - 1; i > 0; i--) {
            if (!numbers.get(i).equals(BigInteger.ZERO)) break;
            numbers.remove(i);
        }
        return numbers;
    }
}
