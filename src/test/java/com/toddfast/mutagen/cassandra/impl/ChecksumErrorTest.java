package com.toddfast.mutagen.cassandra.impl;

import junit.framework.Assert;

import org.junit.Test;

import com.toddfast.mutagen.MutagenException;

public class ChecksumErrorTest extends AbstractTest {

    private final String RESOURCE_PATH = "mutations/tests/checksum_error";

    @Test
    public void cqlChecksumError() throws Exception {

        // Execute mutation with script 1
        mutate(RESOURCE_PATH + "/cql/d1");

        // Mutate again with script 2 (same name, different content), should throw error
        try {
            mutate(RESOURCE_PATH + "/cql/d2");
            Assert.fail("MutagenException expected!");
        } catch (MutagenException e) {
            // expected exception,it is OK
        }

    }

    @Test
    public void javaChecksumError() {

        // First mutation
        mutate(RESOURCE_PATH + "/java/first");

        // Mutate again with file 2 (differs only by its package name), should throw error
        try {
            mutate(RESOURCE_PATH + "/java/second");
            Assert.fail("MutagenException expected!");
        } catch (MutagenException e) {
            // expected exception,it is OK
        }

    }

}
