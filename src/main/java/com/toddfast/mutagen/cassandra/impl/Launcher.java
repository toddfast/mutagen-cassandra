package com.toddfast.mutagen.cassandra.impl;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.toddfast.mutagen.Plan.Result;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.basic.ResourceScanner;
import com.toddfast.mutagen.cassandra.CassandraMutagen;

/*
 * Entry point of mutagen-cassandra executable jar
 */
public class Launcher {

    // Look for mutation scripts in the following folder
    private static final String RESSOURCE_PATH = "com/toddfast/mutagen/cassandra/mutations";

    public static void main(String[] args) {

        // Launch arguments fields
        String propertiesFilePath = null;
        InputStream input = null;
        Properties prop = new Properties();
        System.out.println("Working Directory = " +
                System.getProperty("user.dir"));
        // Datastax resources fields
        Cluster cluster = null;
        Session session = null;
        String clusterContactPoint, clusterPort, useCredentials, dbuser, dbpassword, keyspace;
        // someTests();
        if ((propertiesFilePath = System.getProperty("mutagenCassandra.properties.file")) == null) {
            System.err.println("please provide VM argument \"mutagenCassandra.properties.file\"");
            System.exit(1);
        }

        try {

            input = new FileInputStream(propertiesFilePath);

            // load the properties file
            prop.load(input);

            // get cluster builder
            Cluster.Builder clusterBuilder = Cluster.builder();

            // set contact point
            if ((clusterContactPoint = getUsedProperty(prop, "clusterContactPoint")) != null)
                clusterBuilder = clusterBuilder.addContactPoint(clusterContactPoint);

            // set cluster port if given
            if ((clusterPort = getUsedProperty(prop, "clusterPort")) != null)
                try {
                    clusterBuilder = clusterBuilder.withPort(Integer.parseInt(clusterPort));
                } catch (NumberFormatException e) {
                    System.err.println("Port parameter must be an integer");
                    e.printStackTrace();
                    System.exit(1);
                }

            // set credentials if given
            if ((useCredentials = getUsedProperty(prop, "useCredentials")) != null && useCredentials.matches("true")) {

                if ((dbuser = getUsedProperty(prop, "dbuser")) != null
                        && (dbpassword = getUsedProperty(prop, "dbpassword")) != null)
                    clusterBuilder = clusterBuilder.withCredentials(dbuser, dbpassword);
                else {
                    System.err.println("missing dbuser or dbpassword properties");
                    System.exit(0);
                }

            }

            // build cluster
            cluster = clusterBuilder.build();

            // create session
            if ((keyspace = getUsedProperty(prop, "keyspace")) != null)
                session = cluster.connect(keyspace);
            // session = DatastaxDriverUtils.createSession(DatastaxDriverUtils.createCluster(),keyspace);
            else
                session = cluster.connect();

            // Perform mutations
            CassandraMutagen mutagen = new CassandraMutagenImpl();
            mutagen.initialize(RESSOURCE_PATH);
            Result<String> result = mutagen.mutate(session);

            // print summary
            printMutationResult(result);

        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    // Close file
                    input.close();

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            // close session and cluster
            if (session != null)
                session.close();

            if (cluster != null)
                cluster.close();

        }
    }

    // return Property value if exists and not empty
    private static String getUsedProperty(Properties prop, String name) {
        String s = prop.getProperty(name);
        if (s != null && !s.isEmpty())
            return s;
        else
            return null;

    }

    private static void printMutationResult(Result<String> result) {

        State<Integer> state = result.getLastState();

        System.out.println("Mutation complete: " + result.isMutationComplete());
        System.out.println("Exception: " + result.getException());
        if (result.getException() != null) {
            result.getException().printStackTrace();
        }
        System.out.println("Completed mutations: " + result.getCompletedMutations());
        System.out.println("Remining mutations: " + result.getRemainingMutations());
        System.out.println("Last state: " + (state != null ? state.getID() : "null"));
    }

    private static void someTests() {

        ResourceScanner test = ResourceScanner.getInstance();
        try {
            List<String> res = test.getResources(RESSOURCE_PATH,
                    Pattern.compile(".*"), null);
            for (String s : res) {
                System.out.println(s);
            }
        } catch (URISyntaxException | IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        System.exit(0);
    }
}
