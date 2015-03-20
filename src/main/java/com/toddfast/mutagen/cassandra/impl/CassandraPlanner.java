package com.toddfast.mutagen.cassandra.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.Coordinator;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.Subject;
import com.toddfast.mutagen.basic.BasicPlanner;

/**
 * Generates cassandra migration plans using the initial list of mutations and
 * the specified subject and coordinator.
 */
public class CassandraPlanner extends BasicPlanner<String> {

    /**
     * Constructor for cassandraPlanner.
     * 
     * @param session
     *            the session to execute cql statements.
     * @param mutationResources
     *            script files to migrate.
     *
     */
    protected CassandraPlanner(Session session,
            List<String> mutationResources) {
        super(loadMutations(session, mutationResources), null);
    }

    /**
     * A static method to load mutation for script file(.cqlsh.txt and .java).
     * 
     * @param session
     *            the session to execute cql statements.
     * @param resources
     *            script files to mutate.
     * @return
     *         list of mutation objects.
     */
    private static List<Mutation<String>> loadMutations(
            Session session, Collection<String> resources) {
        List<Mutation<String>> result = new ArrayList<Mutation<String>>();

        for (String resource : resources) {
            // check name of script file
            if (!validate(resource)) {
                System.out.println("script:(" + resource + ") is wrong named!\n");
                throw new IllegalArgumentException("wrong name for " +
                        "mutation resource \"" + resource + "\"");
            }
            // Allow .sql files because some editors have syntax highlighting
            // for SQL but not CQL
            if (resource.endsWith(".cqlsh.txt") || resource.endsWith(".sql")) {
                result.add(
                        new CQLMutation(session, resource));
            }
            else if (resource.endsWith(".class")) {
                result.add(
                        loadMutationClass(session, resource));
            } else if (resource.endsWith(".java")) {
                // ignore java file
                System.out.println("It is a java file.");
            } else {
                throw new IllegalArgumentException("Unknown type for " +
                        "mutation resource \"" + resource + "\"");
            }
        }

        checkForDuplicateRessourceState(result);

        return result;
    }

    private static void checkForDuplicateRessourceState(List<Mutation<String>> mutations) {

        // store all states as string
        List<String> states = new ArrayList<String>();

        for (Mutation<String> m : mutations) {
            states.add(m.getResultingState().getID());
        }

        // create set from states list to get unicity.
        Set<String> set = new HashSet<String>(states);
        if (set.size() < states.size())
            throw new MutagenException("Two migration scripts possess the same state");

    }

    /**
     * validate if the script file is well named(
     * M<DATETIME>_<Camel case title>_<ISSUE>.cqlsh.txt or
     * M<DATETIME>_<Camel case title>_<ISSUE>.java)
     * 
     * @return
     */
    private static boolean validate(String resource) {
        String pattern = "^M(\\d{12})_([a-zA-z]+)_(\\d{4})\\.((java)|(class)|(cqlsh\\.txt))$"; // convention of script
                                                                                               // file
        String fileSeparator = "/"; // file separator
        String resourceName = resource.substring(resource.lastIndexOf(fileSeparator) + 1);

        return resourceName.matches(pattern);
    }

    /**
     * a static method to generate the mutation for script file end with .class
     *
     * @return
     *         mutation for script file end with .class
     */
    private static Mutation<String> loadMutationClass(
            Session session, String resource) {
        assert resource.endsWith(".class") : "Class resource name \"" + resource + "\" should end with .class";

        int index = resource.indexOf(".class");
        String className = resource.substring(0, index).replace('/', '.');

        // Load the class specified by the resource
        Class<?> clazz = null;
        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            // Should never happen
            throw new MutagenException("Could not load mutagen class \"" +
                    resource + "\"", e);
        }

        // Instantiate the class
        try {
            Constructor<?> constructor;
            Mutation<String> mutation = null;
            try {
                // Try a constructor taking a session
                constructor = clazz.getConstructor(Session.class);
                mutation = (Mutation<String>) constructor.newInstance(session);
            } catch (NoSuchMethodException e) {
                // Wrong assumption
            }

            if (mutation == null) {
                // Try the null constructor
                try {
                    constructor = clazz.getConstructor();
                    mutation = (Mutation<String>) constructor.newInstance();
                } catch (NoSuchMethodException e) {
                    throw new MutagenException("Could not find comparible " +
                            "constructor for class \"" + className + "\"", e);
                }
            }

            return mutation;
        } catch (InstantiationException e) {
            throw new MutagenException("Could not instantiate class \"" +
                    className + "\"", e);
        } catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof RuntimeException) {
                throw (RuntimeException) e.getTargetException();
            }
            else {
                throw new MutagenException("Exception instantiating class \"" +
                        className + "\"", e);
            }
        } catch (IllegalAccessException e) {
            throw new MutagenException("Could not access constructor for " +
                    "mutation class \"" + className + "\"", e);
        }
    }

    /**
     * generate mutation context to execute mutations.
     *
     * @return
     *         mutation context
     */
    @Override
    protected Mutation.Context createContext(Subject<String> subject,
            Coordinator<String> coordinator) {
        return new CassandraContext(subject, coordinator);
    }

    /**
     * generate cassandra migration plan for
     * mutating the target subject through a sequence of states.
     * 
     * @return
     *         mutation plan.
     */
    @Override
    public Plan<String> getPlan(Subject<String> subject,
            Coordinator<String> coordinator) {
        return super.getPlan(subject, coordinator);
    }
}
