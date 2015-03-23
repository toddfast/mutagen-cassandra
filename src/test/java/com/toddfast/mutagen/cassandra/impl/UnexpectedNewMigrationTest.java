package com.toddfast.mutagen.cassandra.impl;

import org.junit.Test;

import com.toddfast.mutagen.MutagenException;


public class UnexpectedNewMigrationTest extends AbstractTest {

    /**
     * Check that creating a new migration with older state throws exception.
     */
    @Test(expected = MutagenException.class)
    public void testAddedScriptWithInferiorState() {
        // init
        init();

        createVersionSchemaTable();

        // mutation with versionId ending with 2
        appendOneVersionRecord("201501010002", "Foo", "", 0, true);

        // try to mutate with script ending in 1
        mutate("mutations/tests/unexpected_new_migration");


    }

}
