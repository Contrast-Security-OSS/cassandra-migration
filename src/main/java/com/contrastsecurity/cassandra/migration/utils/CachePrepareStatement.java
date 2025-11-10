package com.contrastsecurity.cassandra.migration.utils;

import java.util.concurrent.ConcurrentHashMap;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

public class CachePrepareStatement {
	private ConcurrentHashMap<Integer, PreparedStatement> cacheStatement = new ConcurrentHashMap<>();

	private final CqlSession session;

	public CachePrepareStatement(CqlSession session) {
		this.session = session;
	}

	public PreparedStatement prepare(String s){
		PreparedStatement ps = cacheStatement.get(s.hashCode());
		if(ps == null){
			ps = session.prepare(s);
			cacheStatement.put(s.hashCode(), ps);
		}
		return ps;
	}
}
