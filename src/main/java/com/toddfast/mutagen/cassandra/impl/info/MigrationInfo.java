package com.toddfast.mutagen.cassandra.impl.info;

import java.util.Date;

public interface MigrationInfo {
    /**
     * @return the timestamp of migration.
     * 
     */
    public String getVersion();

    /**
     * @return the date when the migration was executed.
     * 
     */
    public Date getDate();

    /**
     * @return the filename of the script migration.
     * 
     */
    public String getFilename();

    /**
     * @return if the migration success or not.
     * 
     */
    public boolean getSuccess();

    /**
     * @return the string representation of migration.
     * 
     */
    public String toString();
}
