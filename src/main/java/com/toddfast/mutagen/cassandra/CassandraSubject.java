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
 * 
 * @author Todd Fast
 */
public class CassandraSubject implements Subject<String> {

    /**
	 *
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
	 *
	 *
	 */
    public Session getSession() {
        return session;
    }

    /**
     * create version table
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
     * get current version record
     */

    public ResultSet getVersionRecord() {
        // get version record
        String selectStatement = "SELECT * FROM \"" +
                versionSchemaTable + "\"" +
                limit + ";";
        return session.execute(selectStatement);
    }

    /**
	 * 
	 * 
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
