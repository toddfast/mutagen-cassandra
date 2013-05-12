package com.toddfast.mutagen.cassandra.impl;

import com.conga.nu.AllowField;
import com.conga.nu.Scope;
import com.conga.nu.ServiceProvider;
import com.netflix.astyanax.Keyspace;
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
@ServiceProvider(scope=Scope.CLIENT_MANAGED)
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

			Collections.sort(discoveredResources,COMPARATOR);

			resources=new ArrayList<String>();
			for (String resource: discoveredResources) {
				System.out.println("Found mutation resource \""+resource+"\"");

				if (resource.endsWith(".class")) {
					// Remove the file path
					resource=resource.substring(
						resource.indexOf(rootResourcePath));
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
	public Plan.Result<Integer> mutate(Keyspace keyspace) {

		CassandraCoordinator coordinator=new CassandraCoordinator(keyspace);
		CassandraSubject subject=new CassandraSubject(keyspace);

		List<Mutation<Integer>> mutations=new ArrayList<Mutation<Integer>>();
		Planner<Integer> planner=new CassandraPlanner(keyspace,getResources());

		Plan<Integer> plan=planner.getPlan(subject,coordinator);

		// Execute the plan
		Plan.Result<Integer> result=plan.execute();
		return result;
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

				int index1=path1.lastIndexOf("/");
				int index2=path2.lastIndexOf("/");

				String file1=path1.substring(index1+1);
				String file2=path2.substring(index2+1);

				index1=file1.lastIndexOf(".");
				index2=file2.lastIndexOf(".");

				file1=file1.substring(0,index1);
				file2=file2.substring(0,index2);

				return file1.compareTo(file2);
			}
		};

	@AllowField
	private List<String> resources;
}
