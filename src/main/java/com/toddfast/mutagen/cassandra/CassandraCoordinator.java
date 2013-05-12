package com.toddfast.mutagen.cassandra;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
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
	public CassandraCoordinator(Keyspace keyspace) {
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
	public Keyspace getKeyspace() {
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

	public static final ColumnFamily<String,String> VERSION_CF=
		ColumnFamily.newColumnFamily(
			"schema_version",
			StringSerializer.get(),
			StringSerializer.get());

	private Keyspace keyspace;
}
