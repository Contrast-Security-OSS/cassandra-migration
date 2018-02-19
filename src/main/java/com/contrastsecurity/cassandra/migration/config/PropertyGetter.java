package com.contrastsecurity.cassandra.migration.config;

public class PropertyGetter {

	static public String getProperty(String name, String envName) {
		String propValue = System.getProperty(name);
		if(propValue != null && propValue.length() > 0) return propValue;
		return System.getenv(envName);
	}
}
