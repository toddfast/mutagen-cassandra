package com.toddfast.mutagen.cassandra.impl;

import junit.framework.Assert;

import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;

public class CleanCommandTest extends AbstractTest {
    
    @Test
    public void checkVersionTableDropped() {
        
        // Use working case for test
        mutate("mutations/tests/execution");

        // Clean
        Launcher.clean(getSession());

        // Try select on version table. Should fail since table dropped
        try {
            getSession().execute("SELECT * FROM \"Version\";");
            Assert.fail();
        } catch (InvalidQueryException e) {
            // table has been dropped
        };

    }

}
