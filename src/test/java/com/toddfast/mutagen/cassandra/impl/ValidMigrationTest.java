package com.toddfast.mutagen.cassandra.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.datastax.driver.core.Row;
import com.toddfast.mutagen.Mutation;

/**
 * 
 * 
 */
public class ValidMigrationTest extends AbstractTest {

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

     /**
     * This test tests if the mutation are well done when there are already records in the table Version.
     * We insert a record with the timestamp 201502011209.
     * The script files with timestamp greater than 201502011209 should be executed.
     * The script files with timestamp not greater than 201502011209 should not be executed.
     * 
     */
    @Test
    public void migration_with_record_version_table() {
        // init
        init();

        createVersionSchemaTable();
        appendOneVersionRecord("201502011209", "M201502011209_DoSomeThing_1111.cqlsh.txt", "checksum", 112, true);

        // Execute mutations
        mutate("mutations/tests/execution");

        // Get the mutations executed
        List<String> mutations = new ArrayList<String>();
        for (Mutation<String> mutation : result.getCompletedMutations())
            mutations.add(mutation.getResultingState().getID());

        assertEquals(false, mutations.contains("201502011200"));
        assertEquals(true, mutations.contains("201502011210"));
     }

    /**
     * This test tests if the order of execution is according to the timestamp of script file.
     */
    @Test
    public void migration_execution_order() {
        // init
        init();

        // Execute mutations
        mutate("mutations/tests/execution");

        // Get the mutations executed
        List<String> mutations = new ArrayList<String>();
        for (Mutation<String> mutation : result.getCompletedMutations())
            mutations.add(mutation.getResultingState().getID());

        assertEquals("201502011200", mutations.get(0));
        assertEquals("201502011210", mutations.get(1));
        assertEquals("201502011225", mutations.get(2));
        assertEquals("201502011230", mutations.get(3));
    }

    /**
     * This test tests if it throws the execption when the script file is not named according to name convention.
     */
    @Test(expected = IllegalArgumentException.class)
    public void migration_with_wrong_named_script() {
        // init
        init();

        // Execute mutations
        mutate("mutations/tests/wrongNamedScript");
    }

}
