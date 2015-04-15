package com.toddfast.mutagen.cassandra.impl.info;

public interface MigrationInfoService {
    /**
     * Retriveves the full set of infos.
     * 
     * @return The full set of infos.
     */
    public MigrationInfo[] all();

    /**
     * Retrive the information of the current migration.
     * 
     * @return The info.
     */
    public MigrationInfo current();

    /**
     * Retrive the full set of infos about the migration with the state of success.
     * 
     * @return The full set of infos.
     */
    public MigrationInfo[] success();

    /**
     * Retrive the information of the migration with the state of failure.
     * 
     * @return The info.
     */
    public MigrationInfo failed();

    /**
     * Print the migrations informations.
     * 
     * @return the string representation of migrations.
     */
    public String toString();

    /**
     * Refreshes the info.
     */
    public void refresh();
}
