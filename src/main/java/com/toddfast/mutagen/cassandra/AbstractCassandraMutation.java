package com.toddfast.mutagen.cassandra;

import java.sql.Timestamp;
import java.util.Date;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.PreparedStatement;

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
	 */
	protected AbstractCassandraMutation(Session session) {
		super();
		this.session = session;
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
		int index=versionString.lastIndexOf(fileSeparator);
		if (index!=-1) {
			versionString=versionString.substring(index+1);
		}

		index=versionString.lastIndexOf(cqlMigrationSeparator);
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
	protected Session getSession(){
		return session;
	}

	/**
	 * Perform the actual mutation
	 *
	 */
	protected abstract boolean performMutation(Context context);


	/**
	 *
	 *
	 */
	@Override
	public abstract State<Integer> getResultingState();


	/**
	 * Return a canonical representative of the change in string form
	 *
	 */
	protected abstract String getChangeSummary();

	/**
	 * get the ressource name
	 */
	protected abstract String getRessourceName();
	
	/**
	 * append the version record
	 */
	protected void appendVersionRecord(int version,String filename,String checksum,int execution_time, boolean success){
		//insert version record
		String insertStatement = "INSERT INTO \"" + versionSchemaTable + "\" (versionid,filename,checksum,"
																+ "execution_data,execution_time,success) "
																+ "VALUES (?,?,?,?,?,?);";
		
		PreparedStatement preparedInsertStatement = session.prepare(insertStatement);
		session.execute(preparedInsertStatement.bind(Integer.toString(version), 
													filename, 
													checksum,
													new Timestamp(new Date().getTime()),
													execution_time,
													success
													));
		}
	
	/**
	 * Performs the actual mutation and then updates the recorded schema version
	 *
	 */
	@Override
	public final void mutate(Context context)
			throws MutagenException {

		// Perform the mutation
		long startTime = System.currentTimeMillis();
		boolean success = performMutation(context);
		long endTime = System.currentTimeMillis();
		long execution_time = endTime - startTime;
		
		int version=getResultingState().getID();
		
		System.out.println(version);
		
		String change=getChangeSummary();
		if (change==null) {
			change="";
		}

		String checksum=md5String(change);

		// append version record
		appendVersionRecord(version,getRessourceName(),checksum,(int)execution_time,success);
		
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


	/**
	 *
	 *
	 * @param key
	 * @return
	 */
	public static byte[] md5(String key) {
		MessageDigest algorithm;
		try {
			algorithm=MessageDigest.getInstance("MD5");
		}
		catch (NoSuchAlgorithmException ex) {
			throw new RuntimeException(ex);
		}

		algorithm.reset();

		try {
			algorithm.update(key.getBytes("UTF-8"));
		}
		catch (UnsupportedEncodingException ex) {
			throw new RuntimeException(ex);
		}

		byte[] messageDigest=algorithm.digest();
		return messageDigest;
	}


	/**
	 *
	 *
	 * @param key
	 * @return
	 */
	public static String md5String(String key) {
		byte[] messageDigest=md5(key);
		return toHex(messageDigest);
	}


	/**
	 * Encode a byte array as a hexadecimal string
	 *
	 * @param	bytes
	 * @return
	 */
	public static String toHex(byte[] bytes) {
		StringBuilder hexString=new StringBuilder();
		for (int i=0; i<bytes.length; i++) {

			String hex=Integer.toHexString(0xFF & bytes[i]);
			if (hex.length() == 1) {
				hexString.append('0');
			}

			hexString.append(hex);
		}

		return hexString.toString();
	}




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	private String fileSeparator = "/";		//file separator
	private String cqlMigrationSeparator = "_";  //script separator
	
	private Session session;   //session
	
	private String versionSchemaTable = "Version";

}
