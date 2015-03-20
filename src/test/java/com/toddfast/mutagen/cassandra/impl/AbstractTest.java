package com.toddfast.mutagen.cassandra.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import info.archinnov.achilles.junit.AchillesResource;
import info.archinnov.achilles.junit.AchillesResourceBuilder;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;

import org.junit.After;
import org.junit.Before;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.cassandra.CassandraMutagen;

/**
 * 
 * @author Todd Fast
 */
public abstract class AbstractTest {

    private static String keyspace = "apispark";

    private String versionSchemaTable = "Version";

    public Session session;

    public Plan.Result<String> result;

    /**
     * Using the achilles to create session for test.
     * 
     */
    @Before
    public void setUp() {
        AchillesResource resource = AchillesResourceBuilder
                .noEntityPackages().withKeyspaceName(keyspace).build();

        session = resource.getNativeSession();
    }

    @After
    public void tearDown() {
        session.close();
    }

    /**
     * Get an instance of cassandra mutagen and mutate the mutations.
     * 
     * @return
     *         the result of mutations.
     * 
     */
    protected void mutate(String path) {

        // Get an instance of CassandraMutagen
        CassandraMutagen mutagen = new CassandraMutagenImpl();

        // Initialize the list of mutations
        try {
            mutagen.initialize(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Mutate!
        result = mutagen.mutate(session);
    }

    // CHECK

    protected void checkMutationSuccessful() {

        System.out.println("Mutation complete: " + result.isMutationComplete());
        System.out.println("Exception: " + result.getException());
        if (result.getException() != null) {
            result.getException().printStackTrace();
        }
        System.out.println("Completed mutations: " + result.getCompletedMutations());
        System.out.println("Remining mutations: " + result.getRemainingMutations());
        // System.out.println("Last state: " + (state != null ? state.getID() : "null"));

        // Check for completion and errors
        assertTrue(result.isMutationComplete());
        assertNull(result.getException());
    }

    protected void checkLastTimestamp(String expectedTimestamp) {
        assertEquals(expectedTimestamp, result.getLastState().getID());
    }
    // STATEMENTS

    /**
     * @param values
     *            the values to be binded for a query
     * @return
     *         the result set of a query
     */
    protected ResultSet query(Object... values) {
        String columnFamily = "Test1";
        // query
        String selectStatement = "SELECT * FROM \"" + columnFamily + "\" " + "WHERE key=?";
        PreparedStatement preparedSelectStatement = session.prepare(selectStatement);
        BoundStatement boundSelectStatement = preparedSelectStatement.bind(values);
        return session.execute(boundSelectStatement);
    }

    protected Row getByPk(String pk) {
        ResultSet results = query(pk);
        return results.one();
    }

    protected void createVersionSchemaTable() {
        String dropStatement = "DROP TABLE IF EXISTS \"" + versionSchemaTable + "\";";
        session.execute(dropStatement);
        String createStatement = "CREATE TABLE \"" +
                versionSchemaTable +
                "\"( versionid varchar, filename varchar,checksum varchar,"
                + "execution_date timestamp,execution_time int,"
                + "success boolean, PRIMARY KEY(versionid))";

        session.execute(createStatement);
    }

    protected void appendOneVersionRecord(String version, String filename, String checksum, int execution_time,
            boolean success) {
        // insert version record
        String insertStatement = "INSERT INTO \"" + versionSchemaTable + "\" (versionid,filename,checksum,"
                + "execution_date,execution_time,success) "
                + "VALUES (?,?,?,?,?,?);";

        PreparedStatement preparedInsertStatement = session.prepare(insertStatement);
        session.execute(preparedInsertStatement.bind(version,
                filename,
                checksum,
                new Timestamp(new Date().getTime()),
                execution_time,
                success
                ));
    }

    protected void dropTableTest() {
        String dropStatement = "DROP TABLE \"Test1\";";
        session.execute(dropStatement);
    }
}
