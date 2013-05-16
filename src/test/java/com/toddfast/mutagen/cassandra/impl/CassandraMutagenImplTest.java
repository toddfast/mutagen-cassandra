package com.toddfast.mutagen.cassandra.impl;

import com.toddfast.mutagen.cassandra.CassandraMutagen;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static com.conga.nu.Services.$;
import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.State;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.*;

/**
 *
 * @author Todd Fast
 */
public class CassandraMutagenImplTest {

	public CassandraMutagenImplTest() {
	}


	@BeforeClass
	public static void setUpClass()
			throws Exception {
		defineKeyspace();
		createKeyspace();
	}

	private static void defineKeyspace() {
		context=new AstyanaxContext.Builder()
			.forKeyspace("mutagen_test")
			.withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
//					.setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
//					.setCqlVersion("3.0.0")
//					.setTargetCassandraVersion("1.2")
				.setDefaultReadConsistencyLevel(ConsistencyLevel.CL_QUORUM)
				.setDefaultWriteConsistencyLevel(ConsistencyLevel.CL_QUORUM)
			)
			.withConnectionPoolConfiguration(
				new ConnectionPoolConfigurationImpl("testPool")
				.setPort(9160)
				.setMaxConnsPerHost(1)
				.setSeeds("localhost")
			)
			.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
			.buildKeyspace(ThriftFamilyFactory.getInstance());

		context.start();
		keyspace=context.getClient();
	}

	private static void createKeyspace()
			throws ConnectionException {

		System.out.println("Creating keyspace "+keyspace+"...");

		int keyspaceReplicationFactor=1;

		Map<String, Object> keyspaceConfig=
			new HashMap<String, Object>();
		keyspaceConfig.put("strategy_options",
			ImmutableMap.<String, Object>builder()
				.put("replication_factor",
					""+keyspaceReplicationFactor)
				.build());

		String keyspaceStrategyClass="SimpleStrategy";
		keyspaceConfig.put("strategy_class",keyspaceStrategyClass);

		OperationResult<SchemaChangeResult> result=
			keyspace.createKeyspace(
				Collections.unmodifiableMap(keyspaceConfig));

		System.out.println("Created keyspace "+keyspace);
	}


	@AfterClass
	public static void tearDownClass()
			throws Exception {
		OperationResult<SchemaChangeResult> result=keyspace.dropKeyspace();
		System.out.println("Dropped keyspace "+keyspace);
	}


	@Before
	public void setUp() {
	}


	@After
	public void tearDown() {
	}


	/**
	 * Test of initialize method, of class CassandraMutagenImpl.
	 */
	@Test
	public void testInitialize() throws Exception {

		CassandraMutagen mutagen=$(CassandraMutagen.class);

		String rootResourcePath="db/migrations";
		mutagen.initialize(rootResourcePath);

		Plan.Result<Integer> result=mutagen.mutate(keyspace);

		State<Integer> state=result.getLastState();

		System.out.println("Mutation complete: "+result.isMutationComplete());
		System.out.println("Exception: "+result.getException());
		if (result.getException()!=null) {
			result.getException().printStackTrace();
		}
		System.out.println("Completed mutations: "+result.getCompletedMutations());
		System.out.println("Remining mutations: "+result.getRemainingMutations());
		System.out.println("Last state: "+(state!=null ? state.getID() : "null"));

		assertTrue(result.isMutationComplete());
		assertNull(result.getException());
		assertEquals((state!=null ? state.getID() : (Integer)(-1)),(Integer)4);
	}



	/**
	 * 
	 */
	@Test
	public void testData() throws Exception {

		final ColumnFamily<String,String> CF_TEST1=
			ColumnFamily.newColumnFamily("Test1",
				StringSerializer.get(),StringSerializer.get());

		ColumnList<String> columns;
		columns=keyspace.prepareQuery(CF_TEST1)
			.getKey("row1")
			.execute()
			.getResult();

		assertEquals("foo",columns.getStringValue("value1",null));
		assertEquals("bar",columns.getStringValue("value2",null));

		columns=keyspace.prepareQuery(CF_TEST1)
			.getKey("row2")
			.execute()
			.getResult();

		assertEquals("chicken",columns.getStringValue("value1",null));
		assertEquals("sneeze",columns.getStringValue("value2",null));

		columns=keyspace.prepareQuery(CF_TEST1)
			.getKey("row3")
			.execute()
			.getResult();

		assertEquals("bar",columns.getStringValue("value1",null));
		assertEquals("baz",columns.getStringValue("value2",null));
	}
	
	
	
	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	private static AstyanaxContext<Keyspace> context;
	private static Keyspace keyspace;
}
