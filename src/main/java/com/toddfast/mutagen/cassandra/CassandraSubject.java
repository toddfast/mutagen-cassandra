package com.toddfast.mutagen.cassandra;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.StringSerializer;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.Subject;
import com.toddfast.mutagen.basic.SimpleState;

/**
 *
 * @author Todd Fast
 */
public class CassandraSubject implements Subject<Integer> {

	/**
	 *
	 *
	 */
	public CassandraSubject(Keyspace keyspace) {
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
	private void createSchemaVersionTable()
			throws ConnectionException {
		OperationResult<SchemaChangeResult> result=
			getKeyspace().createColumnFamily(VERSION_CF,null);
	}


	/**
	 * 
	 * 
	 */
	@Override
	public State<Integer> getCurrentState() {

		RowQuery<String,String> query=
			getKeyspace().prepareQuery(VERSION_CF)
				.getKey(ROW_KEY);

		OperationResult<ColumnList<String>> result=null;
		try {
			result=query.execute();
		}
		catch (ConnectionException e) {
			// Probably because the table doesn't exist
			try {
				createSchemaVersionTable();
			}
			catch (ConnectionException ex) {
				throw new MutagenException("Could not create column family "+
					"\"schema_version\"",ex);
			}
		}

		// Now try again
		try {
			result=query.execute();
		}
		catch (ConnectionException e) {
			throw new MutagenException("Could not retrieve version from "+
				"column family \"schema_version\"",e);
		}

		ColumnList<String> columns=result.getResult();
		Integer version=columns.getIntegerValue(VERSION_COLUMN,null);

		if (version==null) {
			// Most likely the column family has only just been created
			version=0;
		}

		return new SimpleState<Integer>(version);
	}




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	public static final ColumnFamily<String,String> VERSION_CF=
		ColumnFamily.newColumnFamily(
			"schema_version",
			StringSerializer.get(),
			StringSerializer.get());
	public static final String ROW_KEY="state";
	public static final String VERSION_COLUMN="version";

	private Keyspace keyspace;
}
