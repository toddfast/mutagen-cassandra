package com.toddfast.mutagen.cassandra;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.basic.SimpleState;

/**
 *
 * @author Todd Fast
 */
public abstract class AbstractCassandraMutation implements Mutation<Integer> {

	/**
	 *
	 *
	 */
	protected AbstractCassandraMutation(Keyspace keyspace) {
		super();
		this.keyspace=keyspace;
	}


	/**
	 *
	 *
	 */
	@Override
	public String toString() {
		if (getResultingState()!=null) {
			return super.toString()+"[state="+getResultingState().getID()+"]";
		}
		else {
			return super.toString();
		}
	}


	/**
	 *
	 *
	 */
	protected final State<Integer> parseVersion(String resourceName) {
		String versionString=resourceName;
		int index=versionString.lastIndexOf("/");
		if (index!=-1) {
			versionString=versionString.substring(index+1);
		}

		index=versionString.lastIndexOf(".");
		if (index!=-1) {
			versionString=versionString.substring(0,index);
		}

		StringBuilder buffer=new StringBuilder();
		for (Character c: versionString.toCharArray()) {
			// Skip all initial non-digit characters
			if (!Character.isDigit(c)) {
				if (buffer.length()==0) {
					continue;
				}
				else {
					// End when we reach the first non-digit
					break;
				}
			}
			else {
				buffer.append(c);
			}
		}

		return new SimpleState<Integer>(Integer.parseInt(buffer.toString()));
	}


	/**
	 *
	 *
	 */
	protected Keyspace getKeyspace() {
		return keyspace;
	}


	/**
	 * Perform the actual mutation
	 *
	 */
	protected abstract void performMutation(Context context);


	/**
	 *
	 *
	 */
	@Override
	public abstract State<Integer> getResultingState();


	/**
	 * Performs the actual mutation and then updates the recorded schema version
	 *
	 */
	@Override
	public final void mutate(Context context)
			throws MutagenException {

		// Perform the mutation
		performMutation(context);

		int version=getResultingState().getID();

		// The straightforward way, without locking
		try {
			MutationBatch batch=getKeyspace().prepareMutationBatch();
			batch
				.withRow(CassandraSubject.VERSION_CF,CassandraSubject.ROW_KEY)
				.putColumn(CassandraSubject.VERSION_COLUMN,version);
			batch.execute();
		}
		catch (ConnectionException e) {
			throw new MutagenException("Could not update \"schema_version\" "+
				"column family to state "+version+
				"; schema is now out of sync with recorded version",e);
		}

// TAF: Why does this fail with a StaleLockException? Do we need to use a
// separate lock table?

//		// Attempt to acquire a lock to update the version
//		ColumnPrefixDistributedRowLock<String> lock =
//			new ColumnPrefixDistributedRowLock<String>(getKeyspace(),
//					CassandraSubject.VERSION_CF,CassandraSubject.VERSION_COLUMN)
//				.withBackoff(new BoundedExponentialBackoff(250, 10000, 10))
//				.expireLockAfter(1, TimeUnit.SECONDS)
////				.failOnStaleLock(false);
//				.failOnStaleLock(true);
//
//		try {
//			lock.acquire();
//		}
//		catch (StaleLockException e) {
//			// Won't happen
//			throw new MutagenException("Could not update "+
//				"\"schema_version\" column family to state "+version+
//				" because lock expired",e);
//		}
//		catch (BusyLockException e) {
//			throw new MutagenException("Could not update "+
//				"\"schema_version\" column family to state "+version+
//				" because another client is updating the recorded version",e);
//		}
//		catch (Exception e) {
//			if (e instanceof RuntimeException) {
//				throw (RuntimeException)e;
//			}
//			else {
//				throw new MutagenException("Could not update "+
//					"\"schema_version\" column family to state "+version+
//					" because a write lock could not be obtained",e);
//			}
//		}
//		finally {
//			try {
//				MutationBatch batch=getKeyspace().prepareMutationBatch();
//				batch.withRow(CassandraSubject.VERSION_CF,
//						CassandraSubject.ROW_KEY)
//					.putColumn(CassandraSubject.VERSION_COLUMN,version);
//
//				// Release and update
//				lock.releaseWithMutation(batch);
//			}
//			catch (Exception e) {
//				if (e instanceof RuntimeException) {
//					throw (RuntimeException)e;
//				}
//				else {
//					throw new MutagenException("Could not update "+
//						"\"schema_version\" column family to state "+version+
//						"; schema is now out of sync with recorded version",e);
//				}
//			}
//		}
	}




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	private Keyspace keyspace;
}
