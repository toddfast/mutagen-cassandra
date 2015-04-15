package com.toddfast.mutagen.cassandra.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import info.archinnov.achilles.junit.AchillesResourceBuilder;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;

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
 * Abstract test class.
 */
public abstract class AbstractTest {
    /**
     * Using the achilles to create a final global session for all tests.
     * 
     */
    private static final Session session = AchillesResourceBuilder
            .noEntityPackages().withKeyspaceName("apispark").build().getNativeSession();

    public Plan.Result<String> result;

    /**
     * initiation for test.
     */
    @Before
    public void init() {
        // purge all table from keyspace
        purgeKeyspace();
    }

    private void purgeKeyspace() {

        // get current keyspace
        String keyspace = session.getLoggedKeyspace();

        // list all tables of keyspace (equivalent to cqlsh DESCRIBE TABLES)
        ResultSet rs = session
                .execute("SELECT columnfamily_name FROM system.schema_columnfamilies where keyspace_name = '"
                        + keyspace + "' ;");

        // drop all tables
        while (!rs.isExhausted()) {
            Row r = rs.one();
            String tablename = r.getString("columnfamily_name");
            session.execute("DROP TABLE \"" + tablename + "\";");
        }
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

    /**
     * check if the mutations are successful.
     */
    protected void checkMutationSuccessful() {
        System.out.println("Mutation complete: " + result.isMutationComplete());
        System.out.println("Exception: " + result.getException());
        if (result.getException() != null) {
            result.getException().printStackTrace();
        }
        System.out.println("Completed mutations: " + result.getCompletedMutations());
        System.out.println("Remaining mutations: " + result.getRemainingMutations());

        // Check for completion and errors
        assertTrue(result.isMutationComplete());
        assertNull(result.getException());
    }

    /**
     * check the last timestamp of migration.
     * 
     * @param expectedTimestamp
     *            the expected timestamp.
     */
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

    /**
     * Get the query result by primary key.
     * 
     * @param pk
     *            primary key.
     * @return
     *         the query result.
     */
    protected Row getByPk(String pk) {
        ResultSet results = query(pk);
        return results.one();
    }

    /**
     * Create version schema table manually.
     */
    protected void createVersionSchemaTable() {
        // create table version
        String createStatement = "CREATE TABLE \"Version\""
                + "( versionid varchar, filename varchar,checksum varchar,"
                + "execution_date timestamp,execution_time int,"
                + "success boolean, PRIMARY KEY(versionid))";

        session.execute(createStatement);
    }

    /**
     * add record in the version table.
     * 
     * @param version
     *            version id.
     * @param filename
     *            script file name.
     * @param checksum
     *            md5 hashage for script file.
     * @param execution_time
     *            execution time(ms) for script file.
     * @param success
     *            if the execution of script file successes.
     */
    protected void appendOneVersionRecord(String version, String filename, String checksum, int execution_time,
            boolean success) {
        // insert version record
        String insertStatement = "INSERT INTO \"Version\" (versionid,filename,checksum,"
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

    public String queryDatabaseForLastState() {
        ResultSet rs = getSession().execute("SELECT versionid FROM \"Version\";");

        String version = "000000000000";

        while (!rs.isExhausted()) {
            Row r = rs.one();
            String versionid = r.getString("versionid");
            if (version.compareTo(versionid) < 0)
                version = versionid;
        }

        return version;

    }

    public Plan.Result<String> getResult() {
        return result;
    }

    /**
     * @return the session
     */
    public static Session getSession() {
        return session;
    }

}
