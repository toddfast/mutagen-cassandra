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
public class CassandraCoordinator implements Coordinator<String> {

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
	public boolean accept(Subject<String> subject,
			State<String> targetState) {
		State<String> currentState=subject.getCurrentState();
		System.out.println("currentstate:" + currentState.getID());
		return (targetState.getID().compareTo(currentState.getID()) >0 );
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
