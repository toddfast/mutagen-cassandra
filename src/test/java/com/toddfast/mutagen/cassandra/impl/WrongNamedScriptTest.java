package com.toddfast.mutagen.cassandra.impl;

import org.junit.Test;

public class WrongNamedScriptTest extends AbstractTest {

    /**
     * This test tests if it throws the execption when the script file is not named according to name convention.
     */
    @Test(expected = IllegalArgumentException.class)
    public void migration_with_wrong_named_script() {

        // Execute mutations
        mutate("mutations/tests/wrongNamedScript");
    }
}
