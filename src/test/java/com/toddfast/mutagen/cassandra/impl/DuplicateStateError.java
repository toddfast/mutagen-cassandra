package com.toddfast.mutagen.cassandra.impl;

import org.junit.Test;

import com.toddfast.mutagen.MutagenException;

public class DuplicateStateError extends AbstractTest {

    /**
     * In case of duplicate states in filename, throws MutagenException
     */
    @Test(expected = MutagenException.class)
    public void test() {
        mutate("mutations/tests/duplicatestate");
        
    }

}
