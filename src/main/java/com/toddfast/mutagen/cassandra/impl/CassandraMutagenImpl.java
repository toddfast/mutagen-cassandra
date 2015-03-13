package com.toddfast.mutagen.cassandra.impl;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.Planner;
import com.toddfast.mutagen.basic.ResourceScanner;
import com.toddfast.mutagen.cassandra.CassandraCoordinator;
import com.toddfast.mutagen.cassandra.CassandraMutagen;
import com.toddfast.mutagen.cassandra.CassandraSubject;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

/**
 *
 * 
 * @author Todd Fast
 */
//@ServiceProvider(scope=Scope.CLIENT_MANAGED)
public class CassandraMutagenImpl implements CassandraMutagen {
	
	/**
	 * 
	 * 
	 */
	@Override
	public void initialize(String rootResourcePath)
			throws IOException {

		try {
			List<String> discoveredResources=
				ResourceScanner.getInstance().getResources(
					rootResourcePath,Pattern.compile(".*"),
					getClass().getClassLoader());

			// Make sure we found some resources
			if (discoveredResources.isEmpty()) {
				throw new IllegalArgumentException("Could not find resources "+
					"on path \""+rootResourcePath+"\"");
			}

			Collections.sort(discoveredResources,COMPARATOR);

			resources=new ArrayList<String>();

			for (String resource: discoveredResources) {
				System.out.println("Found mutation resource \""+resource+"\"");

				if (resource.endsWith(".class")) {
					// Remove the file path
					resource=resource.substring(
						resource.indexOf(rootResourcePath));
					if (resource.contains("$")) {
						// skip inner classes
						continue;
					}
                }

				resources.add(resource);
			}
		}
		catch (URISyntaxException e) {
			throw new IllegalArgumentException("Could not find resources on "+
				"path \""+rootResourcePath+"\"",e);
		}
	}


	/**
	 *
	 *
	 */
	public List<String> getResources() {
		return resources;
	}


	/**
	 *
	 *
	 */
	@Override
	public Plan.Result<Integer> mutate(String keyspace) {
		// Do this in a VM-wide critical section. External cluster-wide 
		// synchronization is going to have to happen in the coordinator.
		synchronized (System.class) {
			cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
			session = cluster.connect(keyspace);
			
			CassandraCoordinator coordinator=new CassandraCoordinator(keyspace);
			CassandraSubject subject=new CassandraSubject(keyspace,session);

			Planner<Integer> planner=
				new CassandraPlanner(keyspace,getResources(),session);
			Plan<Integer> plan=planner.getPlan(subject,coordinator);

			// Execute the plan
			Plan.Result<Integer> result=plan.execute();
			//close session and cluster
			session.close();
			cluster.close();
			
			return result;
		}
	}




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	/**
	 * Sorts by root file name, ignoring path and file extension
	 *
	 */
	private static final Comparator<String> COMPARATOR=
		new Comparator<String>() {
			@Override
			public int compare(String path1, String path2) {
				final String origPath1=path1;
				final String origPath2=path2;

				try {

					int index1=path1.lastIndexOf("/");
					int index2=path2.lastIndexOf("/");

					String file1;
					if (index1!=-1) {
						file1=path1.substring(index1+1);
					}
					else {
						file1=path1;
					}

					String file2;
					if (index2!=-1) {
						file2=path2.substring(index2+1);
					}
					else {
						file2=path2;
					}

					index1=file1.lastIndexOf(".");
					index2=file2.lastIndexOf(".");

					if (index1 > 1) {
						file1=file1.substring(0,index1);
					}

					if (index2 > 1) {
						file2=file2.substring(0,index2);
					}

					return file1.compareTo(file2);
				}
				catch (StringIndexOutOfBoundsException e) {
					throw new StringIndexOutOfBoundsException(e.getMessage()+
						" (path1: \""+origPath1+
						"\", path2: \""+origPath2+"\")");
				}
			}
		};

//	@AllowField
	private List<String> resources;
	
	private String keyspace;
	private Cluster cluster;
	private Session session;
}
