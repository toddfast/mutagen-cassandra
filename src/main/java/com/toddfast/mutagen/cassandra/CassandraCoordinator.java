package com.toddfast.mutagen.cassandra;

import com.toddfast.mutagen.Coordinator;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.Subject;

import com.datastax.driver.core.Session;
/**
 *
 *
 * @author Todd Fast
 */
public class CassandraCoordinator implements Coordinator<Integer> {

	/**
	 * 
	 * 
	 */
	public CassandraCoordinator(Session session) {
		super();
		if (session==null) {
			throw new IllegalArgumentException(
				"Parameter \"session\" cannot be null");
		}

		this.session=session;
	}


	/**
	 *
	 *
	 */
	public Session getSession() {
		return session;
	}


	/**
	 * 
	 * 
	 */
	@Override
	public boolean accept(Subject<Integer> subject,
			State<Integer> targetState) {
		State<Integer> currentState=subject.getCurrentState();
		return targetState.getID() > currentState.getID();
	}




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

//	public static final ColumnFamily<String,String> VERSION_CF=
//		ColumnFamily.newColumnFamily(
//			"schema_version",
//			StringSerializer.get(),
//			StringSerializer.get());

	private Session session;
}
