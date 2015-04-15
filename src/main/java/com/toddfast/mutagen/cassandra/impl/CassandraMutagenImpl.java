package com.toddfast.mutagen.cassandra.impl;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.Planner;
import com.toddfast.mutagen.basic.ResourceScanner;
import com.toddfast.mutagen.cassandra.AbstractCassandraMutation;
import com.toddfast.mutagen.cassandra.CassandraCoordinator;
import com.toddfast.mutagen.cassandra.CassandraMutagen;
import com.toddfast.mutagen.cassandra.CassandraSubject;
import com.toddfast.mutagen.cassandra.impl.info.MigrationInfoService;
import com.toddfast.mutagen.cassandra.impl.info.MigrationInfoServiceImpl;

/**
 * An implementation for cassandraMutagen.
 * It execute all the migration tasks.
 * It is the enter point of application.
 */
public class CassandraMutagenImpl implements CassandraMutagen {

    /**
     * Search the resources(script files) in the path indicated,
     * sort them according their datetime, save them.
     * 
     */
    @Override
    public void initialize(String rootResourcePath)
            throws IOException {

        try {
            List<String> discoveredResources =
                    ResourceScanner.getInstance().getResources(
                            rootResourcePath, Pattern.compile(".*"),
                            getClass().getClassLoader());

            // Make sure we found some resources
            if (discoveredResources.isEmpty()) {
                throw new IllegalArgumentException("Could not find resources " +
                        "on path \"" + rootResourcePath + "\"");
            }
            // Sort the resources with the comparator
            Collections.sort(discoveredResources, COMPARATOR);

            // Clean the resources
            resources = new ArrayList<String>();
            for (String resource : discoveredResources) {
                System.out.println("Found mutation resource \"" + resource + "\"");

                if (resource.endsWith(".class")) {
                    // Remove the file path
                    resource = resource.substring(
                            resource.indexOf(rootResourcePath));
                    if (resource.contains("$")) {
                        // skip inner classes
                        continue;
                    }
                }
                resources.add(resource);
            }
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Could not find resources on " +
                    "path \"" + rootResourcePath + "\"", e);
        }
    }

    /**
     * Return the resources founded.
     *
     * @return resources
     */
    public List<String> getResources() {
        return resources;
    }

    /**
     * Performs the automatic migration tasks.
     * 
     * @return
     *         the results of migration.
     */
    @Override
    public Plan.Result<String> mutate(Session session) {
        // Do this in a VM-wide critical section. External cluster-wide
        // synchronization is going to have to happen in the coordinator.

        synchronized (System.class) {

            CassandraCoordinator coordinator = new CassandraCoordinator(session);
            CassandraSubject subject = new CassandraSubject(session);

            Planner<String> planner =
                    new CassandraPlanner(session, getResources());
            Plan<String> plan = planner.getPlan(subject, coordinator);

            // Execute the plan
            Plan.Result<String> result = plan.execute();

            return result;
        }
    }


    @Override
    public void baseline(Session session, String lastCompletedState) {

        System.out.println("Baseline...");

        synchronized (System.class) {

            CassandraCoordinator coordinator = new CassandraCoordinator(session);
            CassandraSubject subject = new CassandraSubject(session);

            // create Version table if not exists
            subject.createSchemaVersionTable();

            ResultSet rs = session.execute("SELECT * FROM \"Version\";");
            if( !rs.isExhausted())
                throw new MutagenException("Tabble Version is not empty, please clean before executing baseline");

            Planner<String> planner =
                    new CassandraPlanner(session, getResources());
            Plan<String> plan = planner.getPlan(subject, coordinator);

            // Dummy execution of all mutations with state inferior of equal to lastCompletedState
            for (Mutation<String> m : plan.getMutations()) {

                if (m.getResultingState().getID().compareTo(lastCompletedState) <= 0)
                    try {
                        ((AbstractCassandraMutation) m).dummyExecution();
                    } catch (Exception e) {
                        throw new MutagenException("Dummy execution failed for mutation : " + m.toString(), e);
                    }
            }

        }
        System.out.println("Done");

    }

    /**
     * Retrives the complete information about all the migrations.
     * 
     * @param session
     *            - the session to execute the cql.
     * @return instance of MigrationInfoService.
     */
    public MigrationInfoService info(Session session) {
        return new MigrationInfoServiceImpl(session);
    }

    // //////////////////////////////////////////////////////////////////////////
    // Fields
    // //////////////////////////////////////////////////////////////////////////


    /**
     * Sorts by root file name, ignoring path and file extension
     * 
     */
    private static final Comparator<String> COMPARATOR =
            new Comparator<String>() {
                @Override
                public int compare(String path1, String path2) {
                    final String origPath1 = path1;
                    final String origPath2 = path2;

                    try {

                        int index1 = path1.lastIndexOf("/");
                        int index2 = path2.lastIndexOf("/");

                        String file1;
                        if (index1 != -1) {
                            file1 = path1.substring(index1 + 1);
                        }
                        else {
                            file1 = path1;
                        }

                        String file2;
                        if (index2 != -1) {
                            file2 = path2.substring(index2 + 1);
                        }
                        else {
                            file2 = path2;
                        }

                        index1 = file1.lastIndexOf(".");
                        index2 = file2.lastIndexOf(".");

                        if (index1 > 1) {
                            file1 = file1.substring(0, index1);
                        }

                        if (index2 > 1) {
                            file2 = file2.substring(0, index2);
                        }

                        return file1.compareTo(file2);
                    }
                    catch (StringIndexOutOfBoundsException e) {
                        throw new StringIndexOutOfBoundsException(e.getMessage() +
                                " (path1: \"" + origPath1 +
                                "\", path2: \"" + origPath2 + "\")");
                    }
                }
            };

    @Override
    public void clean(Session session) {
        System.out.println("Cleaning...");
        // TRUNCATE instead of drop ?
        session.execute("DROP TABLE IF EXISTS \"Version\";");
        System.out.println("Done");

    }

    @Override
    public void repair(Session session) {
        System.out.println("Repairing...");
        ResultSet rs = session.execute("SELECT * FROM \"Version\";");
        List<Row> selectedRows = new ArrayList<Row>();

        while (!rs.isExhausted()) {
            Row r = rs.one();
            if (!r.getBool("success"))
                selectedRows.add(r);
        }
        rs.all();
        System.out.println(selectedRows.size() + " database entrie(s) have been selected for deletion : ");
        for (Row r : selectedRows) {
            System.out.println(" - " + r.toString());
            session.execute("DELETE FROM \"Version\" WHERE versionid = '" + r.getString("versionid") + "';");
        }
        System.out.println("Done");

    }

    // @AllowField
    private List<String> resources;

}
