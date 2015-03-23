package com.toddfast.mutagen.cassandra.impl;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.toddfast.mutagen.Mutation;

public class MigrationOrderTest extends AbstractTest {
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
}
