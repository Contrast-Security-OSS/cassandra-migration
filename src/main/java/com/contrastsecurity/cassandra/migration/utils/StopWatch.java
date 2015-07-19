package com.contrastsecurity.cassandra.migration.utils;

public class StopWatch {

    private long start;
    private long stop;

    public void start() {
        start = System.currentTimeMillis();
    }

    public void stop() {
        stop = System.currentTimeMillis();
    }

    public long getTotalTimeMillis() {
        return stop - start;
    }
}
