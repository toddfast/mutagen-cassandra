package com.toddfast.mutagen.cassandra.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.datastax.driver.core.Row;

public class MigrationResultValidation extends AbstractTest {
    /**
     * Firstly,This test execute mutations.
     * Then it check the results of mutations.
     * It also checks for database content to verify if the mutations is well done.
     * 
     */
    @Test
    public void migration_with_no_version_table() {
        // init
        init();

        // Execute mutations
        mutate("mutations/tests/simple/cql");

        // Check the results
        checkMutationSuccessful();

        // Check the last timestamp
        checkLastTimestamp("201502011200");

        // Check database content
        // TODO
        Row row1 = getByPk("row1");
        assertNotNull(row1);
        assertEquals("value1", row1.getString("value1"));
    }
}
