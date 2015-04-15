package com.toddfast.mutagen.cassandra.impl;

import junit.framework.Assert;

import org.junit.Test;

public class PreviouslyFailedMutationTest extends AbstractTest {

    /*
     * Test for error flag from previous execution in database
     * There are 3 scripts, the first and last are successful, the second one fails
     */
    @Test
    public void failedMutationShouldThrowError() {

        try {
            mutate("mutations/tests/failed_mutation");
            Assert.assertNotNull(getResult().getException());
        } catch (Exception e) {

        }

        try {
            mutate("mutations/tests/failed_mutation");
            Assert.assertNotNull(getResult().getException());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("There is a failed mutation in database for script "));
            e.printStackTrace();
        }
    }
}
