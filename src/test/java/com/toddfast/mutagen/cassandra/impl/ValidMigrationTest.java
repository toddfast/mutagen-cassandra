package com.toddfast.mutagen.cassandra.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.datastax.driver.core.Row;

/**
 * 
 * @author Todd Fast
 */
public class ValidMigrationTest extends AbstractTest {

    /**
     * Firstly,This test execute mutations.
     * Then it check the results of mutations.
     * It also checks for database content to verify if the mutations is well done.
     * 
     * @throws Exception
     */
    @Test
    public void migration_with_no_version_table() {

        // Execute mutations
        mutate("mutations/tests/simple/cql");

        // Check the results
        checkMutationSuccessful();

        checkLastTimestamp("201502011200");

        // Check database content
        Row row1 = getByPk("row1");
        assertNotNull(row1);
        assertEquals("value1", row1.getString("value1"));
    }
    //
    // @Test
    // /**
    // * This test tests if the mutation are well done when there are already records in the table Version.
    // * We insert a record with the timestamp 201502011200.
    // * The script files with timestamp greater than 201502011200 should be executed.
    // * The script files with timestamp not greater than 201502011200 should not be executed.
    // * @throws Exception
    // */
    // public void testExecutionFiles() throws Exception {
    // createVersionSchemaTable();
    // appendOneVersionRecord("201502011200", "M201502011200_DoSomeThing_1111.cqlsh.txt", "checksum", 112, true);
    // dropTableTest();
    //
    // List<String> mutations = new ArrayList<String>();
    // // Execute mutations
    // Plan.Result<String> result = mutate();
    // // Check the results
    // for (Mutation mutation : result.getCompletedMutations()) {
    // AbstractCassandraMutation m = (AbstractCassandraMutation) mutation;
    // mutations.add(m.getResultingState().getID());
    // }
    // assertEquals(false, mutations.contains("201502011200"));
    // assertEquals(true, mutations.contains("201502011201"));

    // NOT NECESSARY
    // assertEquals(true, mutations.contains("201502011225"));
    // assertEquals(true, mutations.contains("201502011230"));
    // }




}
