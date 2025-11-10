package com.contrastsecurity.cassandra.migration.config;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.join;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toSet;
import static com.contrastsecurity.cassandra.migration.utils.Ensure.notNullOrEmpty;

/**
 * @author Patrick Kranz
 */
public class NetworkStrategy implements ReplicationStrategy {
    private final Map<String, Integer> dataCenters = new HashMap<>();

    @Override
    public String getName() {
        return "NetworkTopologyStrategy";
    }

    @Override
    public String createCqlStatement() {
        if (getDataCenters().isEmpty()) {
            throw new IllegalStateException("There has to be at least one datacenter in order to use NetworkTopologyStrategy.");
        }

        StringBuilder builder = new StringBuilder();
        builder.append("{")
                .append("'class':'").append(getName()).append("',");

        builder.append(join(",",
                dataCenters.keySet().stream().map(dc -> "'" + dc + "':" + dataCenters.get(dc))
                        .collect(Collectors.toSet())));
        builder.append("}");
        return builder.toString();
    }

    public NetworkStrategy with(String datacenter, int replicationFactor) {
        notNullOrEmpty(datacenter, "datacenter");
        if (replicationFactor < 1) {
            throw new IllegalArgumentException("Replication Factor must be greater than zero");
        }
        dataCenters.put(datacenter, replicationFactor);
        return this;
    }

    public Map<String, Integer> getDataCenters() {
        return unmodifiableMap(dataCenters);
    }
}
