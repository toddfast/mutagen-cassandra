package com.toddfast.mutagen.cassandra.impl;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.Coordinator;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.Subject;
import com.toddfast.mutagen.basic.BasicPlanner;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author Todd Fast
 */
public class CassandraPlanner extends BasicPlanner<String> {

	/**
	 *
	 *
	 */
	protected CassandraPlanner(Session session, 
			List<String> mutationResources) {
		super(loadMutations(session,mutationResources),null);
	}


	/**
	 *
	 *
	 */
	private static List<Mutation<String>> loadMutations(
			Session session, Collection<String> resources) {
		List<Mutation<String>> result=new ArrayList<Mutation<String>>();

		for (String resource: resources) {
			// Allow .sql files because some editors have syntax highlighting
			// for SQL but not CQL
			if (resource.endsWith(".cql") || resource.endsWith(".sql")) {
				result.add(
					new CQLMutation(session,resource));
			}
			else
			if (resource.endsWith(".class")) {
				result.add(
					loadMutationClass(session,resource));
			}
			else {
				throw new IllegalArgumentException("Unknown type for "+
					"mutation resource \""+resource+"\"");
			}
		}
		
		return result;
	}


	/**
	 *
	 *
	 */
	private static Mutation<String> loadMutationClass(
			Session session, String resource) {

		assert resource.endsWith(".class"):
			"Class resource name \""+resource+"\" should end with .class";

		int index=resource.indexOf(".class");
		String className=resource.substring(0,index).replace('/','.');

		// Load the class specified by the resource
		Class<?> clazz=null;
		try {
			clazz=Class.forName(className);
		}
		catch (ClassNotFoundException e) {
			// Should never happen
			throw new MutagenException("Could not load mutagen class \""+
				resource+"\"",e);
		}

		// Instantiate the class
		try {
			Constructor<?> constructor;
			Mutation<String> mutation=null;
			try {
				// Try a constructor taking a session
				constructor=clazz.getConstructor(Session.class);
				mutation=(Mutation<String>)constructor.newInstance(session);
			}
			catch (NoSuchMethodException e) {
				// Wrong assumption
			}

			if (mutation==null) {
				// Try the null constructor
				try {
					constructor=clazz.getConstructor();
					mutation=(Mutation<String>)constructor.newInstance();
				}
				catch (NoSuchMethodException e) {
					throw new MutagenException("Could not find comparible "+
						"constructor for class \""+className+"\"",e);
				}
			}

			return mutation;
		}
		catch (InstantiationException e) {
			throw new MutagenException("Could not instantiate class \""+
				className+"\"",e);
		}
		catch (InvocationTargetException e) {
			if (e.getTargetException() instanceof RuntimeException) {
				throw (RuntimeException)e.getTargetException();
			}
			else {
				throw new MutagenException("Exception instantiating class \""+
					className+"\"",e);
			}
		}
		catch (IllegalAccessException e) {
			throw new MutagenException("Could not access constructor for "+
				"mutation class \""+className+"\"",e);
		}
	}


	/**
	 *
	 *
	 */
	@Override
	protected Mutation.Context createContext(Subject<String> subject,
		Coordinator<String> coordinator) {
		return new CassandraContext(subject,coordinator);
	}


	/**
	 *
	 * 
	 */
	@Override
	public Plan<String> getPlan(Subject<String> subject,
			Coordinator<String> coordinator) {
		return super.getPlan(subject,coordinator);
	}
}
