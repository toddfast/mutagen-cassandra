package com.toddfast.mutagen.cassandra;

import java.io.IOException;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.cassandra.impl.info.MigrationInfoService;

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

    /**
     * @param session
     *            the session to execute cql statements, just one instance in the application.
     * 
     */
    public void baseline(Session session, String lastCompletedState);

    /**
     * @param session
     *            the session to execute cql statements, just one instance in the application.
     * 
     */
    public void clean(Session session);

    /**
     * @param session
     *            the session to execute cql statements, just one instance in the application.
     * 
     */
    public void repair(Session session);

    /**
     * Retrive the complete information about all the migrations.
     * 
     * @param session
     *            - the session to execute cql.
     * @return instance of MigrationInfoService.
     * 
     */
    public MigrationInfoService info(Session session);

}
