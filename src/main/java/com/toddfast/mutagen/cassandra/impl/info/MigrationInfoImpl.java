package com.toddfast.mutagen.cassandra.impl.info;

import java.util.Date;

import com.datastax.driver.core.Row;

public class MigrationInfoImpl implements MigrationInfo {
    /*
     * version record
     */
    private Row row;

    /*
     * Create a new MigrationInfoImpl.
     * 
     * @param row - a version record in the version table.
     */
    public MigrationInfoImpl(Row row) {
        this.row = row;
    }

    @Override
    public String getVersion() {
        return row.getString("versionid");
    }

    @Override
    public Date getDate() {
        return row.getDate("execution_date");
    }

    @Override
    public String getFilename() {
        return row.getString("filename");
    }

    @Override
    public boolean getSuccess() {
        return row.getBool("success");
    }

    @Override
    public String toString() {
        return getVersion()
                + " | " + getDate()
                + " | " + getFilename()
                + " | " + (getSuccess() ? "success" : "failed")
                + " | \n";
    };
}
