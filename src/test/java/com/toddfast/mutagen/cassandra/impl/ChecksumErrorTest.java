package com.toddfast.mutagen.cassandra.impl;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.List;
import java.util.regex.Pattern;

import junit.framework.Assert;

import org.junit.Test;

import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.basic.ResourceScanner;

public class ChecksumErrorTest extends AbstractTest {

    private final String RESOURCE_PATH = "mutations/tests/checksum_error";

    private final String CONTENT_V1 = "create table \"Test1\" (key varchar PRIMARY KEY,value1 varchar);";

    private final String CONTENT_V2 = "create table \"Test2\" (key varchar PRIMARY KEY,value1 varchar);";

    private List<String> discoveredResources;

    @Test
    public void cqlChecksumError() throws Exception {

        // Storing list of absolute paths for resources
        discoveredResources =
                ResourceScanner.getInstance().getResources(
                        RESOURCE_PATH + "/cql", Pattern.compile(".*"),
                        getClass().getClassLoader());

        // if there's exactly one file in the folder
        if (discoveredResources == null || discoveredResources.size() != 1) {
            throw new RuntimeException("There should be exactly one file in " + RESOURCE_PATH);
        }

        writeToFile(discoveredResources.get(0), CONTENT_V1);

        // Execute mutation
        mutate(RESOURCE_PATH + "/cql");

        // Overwrite file with different content
        writeToFile(discoveredResources.get(0), CONTENT_V2);

        // Mutate again to control, should throw error
        try {
            mutate(RESOURCE_PATH + "/cql");
            Assert.fail("MutagenException expected!");
        } catch (MutagenException e) {
            // expected exception,it is OK
        }

    }

    @Test(expected = MutagenException.class)
    public void javaChecksumError() {

        try {

            // Storing list of absolute paths for resources
            discoveredResources =
                    ResourceScanner.getInstance().getResources(
                            RESOURCE_PATH + "/java", Pattern.compile(".*"),
                            getClass().getClassLoader());

        } catch (URISyntaxException | IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        // if there's exactly one file in the folder
        if (discoveredResources != null && discoveredResources.size() == 2) {

            // First mutation
            mutate(RESOURCE_PATH + "/java");

            // Modification to java file
            for (String s : discoveredResources) {
                if (s.endsWith(".java")) {
                    try {
                        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(s, true)));
                        writer.println("//addition");
                        writer.close();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        throw new RuntimeException("could not write to file");
                    }
                }
            }

            // Mutate again to control, should throw error
            mutate(RESOURCE_PATH + "/java");

        }

    }

    private void writeToFile(String filename, String content) throws IOException, URISyntaxException {

        FileWriter writer = new FileWriter(filename);
        writer.write(content);
        writer.close();
    }

    public InputStream getJavaClassInputStream(Class<?> myClass) throws IOException {
        String path = myClass.getName().replace('.', '/');
        String fileName = new StringBuffer(path).append(".java").toString();
        return myClass.getClassLoader().getResourceAsStream(fileName);
    }
}
