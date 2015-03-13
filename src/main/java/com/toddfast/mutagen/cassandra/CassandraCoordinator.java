package com.toddfast.mutagen.cassandra;

import com.toddfast.mutagen.Coordinator;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.Subject;

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
	public CassandraCoordinator(String keyspace) {
		super();
		if (keyspace==null) {
			throw new IllegalArgumentException(
				"Parameter \"keyspace\" cannot be null");
		}

		this.keyspace=keyspace;
	}


	/**
	 *
	 *
	 */
	public String getKeyspace() {
		return keyspace;
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

	private String keyspace;
}
