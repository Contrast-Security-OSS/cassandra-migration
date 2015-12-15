package com.contrastsecurity.cassandra.migration;

import com.contrastsecurity.cassandra.migration.config.Keyspace;
import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;
import com.contrastsecurity.cassandra.migration.logging.console.ConsoleLog;
import com.contrastsecurity.cassandra.migration.logging.console.ConsoleLogCreator;

import java.util.ArrayList;
import java.util.List;

public class CommandLine {
    private static Log LOG;

    /**
     * command to trigger validate action
     */
    public static final String CLEAN = "clean";

    public static void main(String[] args) {
        ConsoleLog.Level logLevel = getLogLevel(args);
        initLogging(logLevel);

        List<String> operations = determineOperations(args);
        if (operations.isEmpty()) {
            printUsage();
            return;
        }

        String operation = operations.get(0);

        CassandraMigration cm = new CassandraMigration();
        Keyspace ks = new Keyspace();
        cm.setKeyspace(ks);

        if (CLEAN.equalsIgnoreCase(operation)) {
            cm.clean();
        }
    }

    private static List<String> determineOperations(String[] args) {
        List<String> operations = new ArrayList<>();

        for (String arg : args) {
            if (!arg.startsWith("-")) {
                operations.add(arg);
            }
        }

        return operations;
    }

    static void initLogging(ConsoleLog.Level level) {
        LogFactory.setLogCreator(new ConsoleLogCreator(level));
        LOG = LogFactory.getLog(CommandLine.class);
    }

    private static ConsoleLog.Level getLogLevel(String[] args) {
        for (String arg : args) {
            if ("-X".equals(arg)) {
                return ConsoleLog.Level.DEBUG;
            }
            if ("-q".equals(arg)) {
                return ConsoleLog.Level.WARN;
            }
        }
        return ConsoleLog.Level.INFO;
    }

    private static void printUsage() {
        LOG.info("********");
        LOG.info("* Usage");
        LOG.info("********");
        LOG.info("");
        LOG.info("cassandra-migration [options] command");
        LOG.info("");
        LOG.info("Commands");
        LOG.info("========");
        LOG.info("migrate  : Migrates the database");
        LOG.info("");
        LOG.info("clean : drops all the tables in a keyspace");
        LOG.info("");
        LOG.info("Add -X to print debug output");
        LOG.info("Add -q to suppress all output, except for errors and warnings");
        LOG.info("");
    }
}
