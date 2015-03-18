package com.toddfast.mutagen.cassandra.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import info.archinnov.achilles.junit.AchillesResource;
import info.archinnov.achilles.junit.AchillesResourceBuilder;

import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.cassandra.CassandraMutagen;

/**
 * 
 * @author Todd Fast
 */
public class CassandraMutagenImplTest {


    /**
     * This is it!
     * 
     */
    private Plan.Result<String> mutate()
            throws IOException {

        // Get an instance of CassandraMutagen
        // Using Nu: CassandraMutagen mutagen=$(CassandraMutagen.class);
        CassandraMutagen mutagen = new CassandraMutagenImpl();

        // Initialize the list of mutations
        String rootResourcePath = "com/toddfast/mutagen/cassandra/test/mutations";
        mutagen.initialize(rootResourcePath);

        // Mutate!
        Plan.Result<String> result = mutagen.mutate(session);

        return result;
    }



    /**
	 *
	 *
	 */
    public ResultSet query(Object... values) {
        String columnFamily = "Test1";
        // query
        String selectStatement = "SELECT * FROM \"" + columnFamily + "\" " + "WHERE key=?";
        PreparedStatement preparedSelectStatement = session.prepare(selectStatement);
        BoundStatement boundSelectStatement = preparedSelectStatement.bind(values);
        return session.execute(boundSelectStatement);
    }

    @Test
    public void testData() throws Exception {

        //Execute mutations
        Plan.Result<String> result = mutate();

        // Check the results
        State<Integer> state = result.getLastState();

        System.out.println("Mutation complete: " + result.isMutationComplete());
        System.out.println("Exception: " + result.getException());
        if (result.getException() != null) {
            result.getException().printStackTrace();
        }
        System.out.println("Completed mutations: " + result.getCompletedMutations());
        System.out.println("Remining mutations: " + result.getRemainingMutations());
        System.out.println("Last state: " + (state != null ? state.getID() : "null"));

        
        // Check for completion and errors
        assertTrue(result.isMutationComplete());
        assertNull(result.getException());
        assertEquals((state != null ? state.getID() : "000000000000"), "201502011230");
        
        
        
        // Check database content
        ResultSet results1 = query("row1");
        Row row1 = results1.one();
        assertEquals("foo", row1.getString("value1"));
        assertEquals("bar", row1.getString("value2"));

        ResultSet results2 = query("row2");
        Row row2 = results2.one();
        assertEquals("chicken", row2.getString("value1"));
        assertEquals("sneeze", row2.getString("value2"));

        ResultSet results3 = query("row3");
        Row row3 = results3.one();

        assertEquals("bar", row3.getString("value1"));
        assertEquals("baz", row3.getString("value2"));
    }


    // //////////////////////////////////////////////////////////////////////////
    // Fields
    // //////////////////////////////////////////////////////////////////////////

    private static String keyspace = "apispark";

    @Rule
    public AchillesResource resource = AchillesResourceBuilder
            .noEntityPackages().withKeyspaceName(keyspace).build();

    public Session session = resource.getNativeSession();
}
