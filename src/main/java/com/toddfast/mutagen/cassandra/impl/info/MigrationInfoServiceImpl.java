package com.toddfast.mutagen.cassandra.impl.info;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;

public class MigrationInfoServiceImpl implements MigrationInfoService {

    private List<MigrationInfoImpl> migrationInfos = new ArrayList<MigrationInfoImpl>();

    private Session session;

    private final Comparator<Row> COMPARATOR = new Comparator<Row>() {
        @Override
        public int compare(Row row1, Row row2) {
            return row1.getString("versionid").compareTo(row2.getString("versionid"));
        }
    };

    /**
     * MigrationInfoServiceImpl constructor.
     * 
     * @param session
     *            - the session to execute the cql.
     */
    public MigrationInfoServiceImpl(Session session) {
        this.session = session;
    }

    /**
     * Get all the records in version table.
     * 
     * @return all the records in version tables.
     */
    public ResultSet getAllRecords() {
        // get version record
        ResultSet resultSet = null;
        String selectStatement = "SELECT * FROM \"" +
                "Version" + "\"" +
                " limit 1000000000 " + ";";
        try {
            resultSet = session.execute(selectStatement);
        } catch (InvalidQueryException e) {
            System.out.println("Warnning : The version table is not created!!!");
        }
        return resultSet;
    }

    /**
     * set the migration info array.
     * 
     * @param resultSet
     *            - all the record in the version table.
     */
    public void setMigrations(ResultSet resultSet) {
        if (resultSet != null) {
            Row[] rows = resultSet.all().toArray(new Row[resultSet.all().size()]);
            Arrays.sort(rows, COMPARATOR);
            for (int i = 0; i < rows.length; i++)
                migrationInfos.add(new MigrationInfoImpl(rows[i]));
        }
    }

    @Override
    public void refresh() {
        if (migrationInfos != null)
            this.migrationInfos.clear();
        setMigrations(getAllRecords());
    }

    @Override
    public MigrationInfo[] all() {
        return migrationInfos.toArray(new MigrationInfoImpl[migrationInfos.size()]);
    }

    @Override
    public MigrationInfo current() {
        return migrationInfos.size() > 0 ? migrationInfos.get(migrationInfos.size() - 1) : null;
    }

    @Override
    public MigrationInfo[] success() {
        List<MigrationInfoImpl> successMigrations = new ArrayList<MigrationInfoImpl>();
        for (MigrationInfoImpl migrationInfoImpl : migrationInfos) {
            if (migrationInfoImpl.getSuccess())
                successMigrations.add(migrationInfoImpl);
        }
        return successMigrations.toArray(new MigrationInfoImpl[successMigrations.size()]);
    }

    @Override
    public MigrationInfo failed() {
        if (migrationInfos.get(migrationInfos.size() - 1).getSuccess())
            return null;
        return migrationInfos.get(migrationInfos.size() - 1);
    }

    @Override
    public String toString() {
        if (migrationInfos.isEmpty())
            return "no migrations found";

        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("Info.....\n");

        for (MigrationInfoImpl migrationInfoImpl : migrationInfos) {
            stringBuffer.append(migrationInfoImpl.toString());
            }

        return stringBuffer.toString();
    }
}
