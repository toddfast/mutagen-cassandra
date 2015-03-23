package com.toddfast.mutagen.cassandra.impl;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ DuplicateStateError.class,
        MigrationOrderTest.class,
        MigrationResultValidation.class,
        MigrationWithRecordInVersionTableTest.class,
        UnexpectedNewMigrationTest.class,
        WrongNamedScriptTest.class,
        WrongCqlScriptFileTest.class,
        WrongJavaScriptFileTest.class,
        ChecksumErrorTest.class })

public class TestSuiteForAllTests {
    // test suite
}
