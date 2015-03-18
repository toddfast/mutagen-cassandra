package com.toddfast.mutagen.cassandra;

import java.io.IOException;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.Plan;

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
    public Plan.Result<String> mutate(Session session);
}
