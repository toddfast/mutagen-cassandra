package com.toddfast.mutagen.cassandra;

import com.netflix.astyanax.Keyspace;
import com.toddfast.mutagen.Plan;
import java.io.IOException;

/**
 *
 *
 * @author Todd Fast
 */
public interface CassandraMutagen {

	/**
	 *
	 *
	 */
	public void initialize(String rootResourcePath)
		throws IOException;


	/**
	 *
	 *
	 */
	public Plan.Result<Integer> mutate(Keyspace keyspace);
}
