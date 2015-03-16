package com.toddfast.mutagen.cassandra;

import com.toddfast.mutagen.Plan;
import java.io.IOException;
import com.datastax.driver.core.Session;
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
	public Plan.Result<Integer> mutate(Session session);
}
