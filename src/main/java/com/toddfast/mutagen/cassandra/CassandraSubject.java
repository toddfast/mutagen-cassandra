package com.toddfast.mutagen.cassandra;

import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.Subject;
import com.toddfast.mutagen.basic.SimpleState;

/**
 * Cassandra subject represents the table Version.
 * It executes the tasks related with the table version: <br>
 * create version table <br>
 * query version table <br>
 * get the current datebase timestamp in the version table<br>
 * 
 */
public class CassandraSubject implements Subject<String> {

    /**
     * Constructor for cassandraSubjet.
     * 
     * @param session
     *            the session to execute cql statements.
     * 
     */
    public CassandraSubject(Session session) {
        super();
        if (session == null) {
            throw new IllegalArgumentException(
                    "Parameter \"keyspace\" cannot be null");
        }

        this.session = session;
        this.version = "000000000000";
    }

    /**
     * a getter method to get session.
     * 
     * @return session
     */
    public Session getSession() {
        return session;
    }

    /**
     * Create table Version.
     * 
     */
    public void createSchemaVersionTable() {
        // createstatement
        String createStatement = "CREATE TABLE \"" +
                versionSchemaTable +
                "\"( versionid varchar, filename varchar,checksum varchar,"
                + "execution_date timestamp,execution_time int,"
                + "success boolean, PRIMARY KEY(versionid))";

        session.execute(createStatement);
    }

    /**
     * Execute query in the table Version.
     * 
     * @return
     *         all the records in version table.
     */

    public ResultSet getVersionRecord() {
        // get version record
        String selectStatement = "SELECT * FROM \"" +
                versionSchemaTable + "\"" +
                limit + ";";
        return session.execute(selectStatement);
    }

    /**
     * Find record for a given versionId
     * 
     * @param versionId
     * @return Result set with one row if versionId present, empty otherwise
     */
    public ResultSet getVersionRecordByVersionId(String versionId) {
        String selectStatement = "SELECT * FROM \"" +
                versionSchemaTable + "\" WHERE versionid = '" + versionId + "'";
        return session.execute(selectStatement);
    }

    /**
     * 
     * @param versionId
     * @return true if versionId is in the database
     */
    public boolean isVersionIdPresent(String versionId) {
        return !getVersionRecordByVersionId(versionId).all().isEmpty();
    }

    public boolean isMutationHashCorrect(String versionId, String hash) {
        String selectStatement = "SELECT checksum FROM \"" +
                versionSchemaTable + "\" WHERE versionid = '" + versionId + "'";
        ResultSet result = session.execute(selectStatement);

        String checksum = result.all().get(0).getString("checksum");

        return (checksum.compareTo(hash) == 0);
    }

    /**
     * Get the current timestamp in the database.
     * 
     * @return
     *         the current timestamp in the database.
     */
    @Override
    public State<String> getCurrentState() {
        if (version == "000000000000") {
            ResultSet results = null;
            try {
                results = getVersionRecord();
            } catch (Exception e) {
                try {
                    createSchemaVersionTable();
                } catch (Exception e2) {
                    throw new MutagenException("Could not create version table", e2);
                }
            }
            try {
                results = getVersionRecord();
            } catch (Exception e) {
                throw new MutagenException(
                        "could not retreive Version table information", e);

            }

            List<Row> rows = results.all();

            for (Row r1 : rows) {
                String versionid = r1.getString("versionid");
                if (version.compareTo(versionid) < 0)
                    version = versionid;
            }
        }
        return new SimpleState<String>(version);
    }

    // //////////////////////////////////////////////////////////////////////////
    // Fields
    // //////////////////////////////////////////////////////////////////////////

    private Session session; // session

    private String version; // current version

    private String versionSchemaTable = "Version";

    private String limit = " limit " + 1_000_000_000;
}
