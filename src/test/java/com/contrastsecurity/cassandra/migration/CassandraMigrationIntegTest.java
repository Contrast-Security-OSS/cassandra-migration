package com.contrastsecurity.cassandra.migration;

import com.contrastsecurity.cassandra.migration.info.MigrationInfo;
import com.contrastsecurity.cassandra.migration.info.MigrationInfoDumper;
import com.contrastsecurity.cassandra.migration.info.MigrationInfoService;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.anyOf;

public class CassandraMigrationIntegTest extends BaseIntegTest {

    @Test
    public void migrationTest() {
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
        }
    }
}
