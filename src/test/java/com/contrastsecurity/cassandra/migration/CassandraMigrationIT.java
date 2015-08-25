package com.contrastsecurity.cassandra.migration;

import com.contrastsecurity.cassandra.migration.info.MigrationInfo;
import com.contrastsecurity.cassandra.migration.info.MigrationInfoDumper;
import com.contrastsecurity.cassandra.migration.info.MigrationInfoService;
import org.junit.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.notNullValue;

public class CassandraMigrationIT extends BaseIT {

    @Test
    public void runApiTest() {
        String[] scriptsLocations = {"migration/integ"};

        CassandraMigration cm = new CassandraMigration();
        cm.getConfigs().setScriptsLocations(scriptsLocations);
        cm.setKeyspace(getKeyspace());
        cm.migrate();

        MigrationInfoService infoService = cm.info();
        System.out.println(MigrationInfoDumper.dumpToAsciiTable(infoService.all()));
        for(MigrationInfo info : infoService.all()) {
            assertThat(info.getVersion().getVersion(), anyOf(is("1.0.0"), is("2.0.0")));
            assertThat(info.getDescription(), anyOf(is("First"), is("Second")));
            assertThat(info.getState().isApplied(), is(true));
            assertThat(info.getInstalledOn(), notNullValue());
        }
    }

    static boolean runCmdTestCompleted = false;
    static boolean runCmdTestSuccess = false;
    @Test
    public void runCmdTest() throws IOException, InterruptedException {
        String shell =
                "java -jar" +
                        " -Dcassandra.migration.scripts.locations=filesystem:target/test-classes/migration/integ" +
                        " -Dcassandra.migration.cluster.contactpoints=" + BaseIT.CASSANDRA_CONTACT_POINT +
                        " -Dcassandra.migration.cluster.port=" + BaseIT.CASSANDRA_PORT +
                        " -Dcassandra.migration.cluster.username=" + BaseIT.CASSANDRA_USERNAME +
                        " -Dcassandra.migration.cluster.password=" + BaseIT.CASSANDRA_PASSWORD +
                        " -Dcassandra.migration.keyspace.name=" + BaseIT.CASSANDRA__KEYSPACE +
                        " target/*-jar-with-dependencies.jar" +
                        " migrate";
        ProcessBuilder builder;
        if(isWindows()) {
            throw new NotImplementedException();
        } else {
            builder = new ProcessBuilder("bash", "-c", shell);
        }
        builder.redirectErrorStream(true);
        final Process process = builder.start();

        watch(process);

        while(!runCmdTestCompleted)
            Thread.sleep(1000L);

        assertThat(runCmdTestSuccess, is(true));
    }

    private static void watch(final Process process) {
        new Thread(new Runnable() {
            public void run() {
                BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
                String line;
                try {
                    while ((line = input.readLine()) != null) {
                        if(line.contains("Successfully applied 2 migrations"))
                            runCmdTestSuccess = true;
                        //System.out.println(line);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                runCmdTestCompleted = true;
            }
        }).start();
    }

    private boolean isWindows() {
        return (System.getProperty("os.name").toLowerCase()).contains("win");
    }
}
