package com.toddfast.mutagen.cassandra;

import java.io.IOException;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.Plan;

/**
 * An interface that runs the cassandra migration tasks.
 * The application should implement this interface to finish automatic migration.
 */
public interface CassandraMutagen {

    /**
     * 
     * @param rootResourcePath
     *            the path where the scripts files(.cqlsh.txt and .java) are located.
     * @throws IOException
     *             IO Exception
     */
    public void initialize(String rootResourcePath)
            throws IOException;

    /**
     * @param session
     *            the session to execute cql statements, just one instance in the application.
     * @return
     *         the result of migration of all the scripts.
     */
    public Plan.Result<String> mutate(Session session);
}
