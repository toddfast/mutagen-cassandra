package com.toddfast.mutagen.cassandra.impl;

import junit.framework.Assert;

import org.junit.Test;

import com.datastax.driver.core.ResultSet;

public class repairCommandTest extends AbstractTest {

    @Test
    public void testFailuresRemoved() {

        // mutate with failure
        mutate("mutations/tests/failed_mutation");
        Assert.assertNotNull(getResult().getException());

        // Make sure there was a recorded failure
        ResultSet rs = getSession().execute("SELECT success FROM \"Version\"");

        boolean mutationHasFailed = false;
        while (!rs.isExhausted()) {
            if(!rs.one().getBool(0))
                mutationHasFailed = true;
        }
        
        Assert.assertTrue(mutationHasFailed);

        // Repair
        Launcher.repair(getSession());

        // make sure there's no failure in the database
        rs = getSession().execute("SELECT success FROM \"Version\"");

        while (!rs.isExhausted()) {
            if (!rs.one().getBool(0))
                Assert.fail();
        }

    }

}
