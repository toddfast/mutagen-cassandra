package com.toddfast.mutagen.cassandra;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.util.Date;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.basic.SimpleState;

/**
 * Base class for cassandra mutation.
 * An {@link Mutation} implementation for cassandra.
 * Represents a single change that can be made to a resource,identified
 * unambiguously by a state.
 * 
 */
public abstract class AbstractCassandraMutation implements Mutation<String> {
    /**
     * Constructor for AbstractCassandraMutation.
     * 
     * @param session
     *            the session to execute cql statement
     */
    public AbstractCassandraMutation(Session session) {

        this.session = session;
        this.state = null;
    }

    /**
     * Get the string of mutation state.
     * 
     * @return string representing the mutation state.
     */
    @Override
    public String toString() {
        if (getResultingState() != null) {
            return getResourceName() + "[state=" + getResultingState().getID() + "]";
        }
        else {
            return getResourceName();
        }
    }

    /**
     * Returns the state of a resource.
     * The state represents the datetime of the resource with the name convention:<br>
     * M<DATETIME>_<Camel case title>_<ISSUE>.cqlsh.txt<br>
     * M<DATETIME>_<Camel case title>_<ISSUE>.java<br>
     * 
     * @param resourceName
     *            the name of resource.
     * @return
     *         the state of a resource.
     */
    protected final State<String> parseVersion(String resourceName) {
        String versionString = resourceName;
        int index = versionString.lastIndexOf(fileSeparator);
        if (versionString.lastIndexOf(fileSeparator) != -1) {
            versionString = versionString.substring(index + 1);
        }

        index = versionString.lastIndexOf(cqlMigrationSeparator);
        if (index != -1) {
            versionString = versionString.substring(0, index);
        }

        StringBuilder buffer = new StringBuilder();
        for (Character c : versionString.toCharArray()) {
            // Skip all initial non-digit characters
            if (!Character.isDigit(c)) {
                if (buffer.length() == 0) {
                    continue;
                }
                else {
                    // End when we reach the first non-digit
                    break;
                }
            }
            else {
                buffer.append(c);
            }
        }

        return new SimpleState<String>(buffer.toString());
    }

    /**
     * A getter method for session.
     * 
     * @return
     */
    protected Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }
    /**
     * Override to perform the actual mutation.
     * 
     * @param context
     *            Logs to {@link System.out} and {@link System.err}
     */
    protected abstract void performMutation(Context context);

    /**
     * Get the state after mutation.
     * 
     * @return state
     */
    @Override
    public State<String> getResultingState() {
        if (state != null)
            return state;
        else
            return state = parseVersion(getResourceName());
    }

    /**
     * Override to get the name of resource.
     * 
     * @return
     */
    protected abstract String getResourceName();

    /**
     * append the version record in the table Version.
     * 
     * @param version
     *            Id of version record,usually represented by the datetime.
     * @param filename
     *            name of script file that was executed.
     * @param checksum
     *            checksum for validation.
     * @param execution_time
     *            The execution time(ms) for this script file.
     * @param success
     *            represents if this execution successes.
     */
    protected void appendVersionRecord(String version, String filename, String checksum, int execution_time,
            boolean success) {
        // insert statement for version record
        String insertStatement = "INSERT INTO \"" + versionSchemaTable + "\" (versionid,filename,checksum,"
                + "execution_date,execution_time,success) "
                + "VALUES (?,?,?,?,?,?);";
        // prepare statement
        PreparedStatement preparedInsertStatement = session.prepare(insertStatement);
        session.execute(preparedInsertStatement.bind(version,
                filename,
                checksum,
                new Timestamp(new Date().getTime()),
                execution_time,
                success
                ));
    }

    /**
     * Performs the actual mutation and then updates the recorded schema version.
     * 
     */
    @Override
    public final void mutate(Context context)
            throws MutagenException {

        // Perform the mutation
        boolean success = true;
        long startTime = System.currentTimeMillis();
        try {
            performMutation(context);
        } catch (MutagenException e) {
            success = false;
            throw e;
        }


        long endTime = System.currentTimeMillis();
        long execution_time = endTime - startTime;

        String version = getResultingState().getID();

        // caculate the checksum
        String checksum = getChecksum();

        // append version record
        appendVersionRecord(version, getResourceName(), checksum, (int) execution_time, success);

    }

    /**
     * 
     * @return the MD5 hash of the current mutation
     */
    public abstract String getChecksum();

    /**
     * Generate the MD5 hash for a key.
     * 
     * @param key
     *            the string to be hashed.
     * @return
     *         the MD5 hash for the key.
     */
    public static byte[] md5(String key) {
        MessageDigest algorithm;
        try {
            algorithm = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }

        algorithm.reset();

        try {
            algorithm.update(key.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }

        byte[] messageDigest = algorithm.digest();
        return messageDigest;
    }

    /**
     * change the hash of a key into hexadecimal format
     * 
     * @param key
     *            the string to be hashed.
     * @return
     *         the hexadecimal format of hash of a key.
     */
    public static String md5String(String key) {
        byte[] messageDigest = md5(key);
        return toHex(messageDigest);
    }

    /**
     * Encode a byte array as a hexadecimal string
     * 
     * @param bytes
     *            byte array
     * @return
     *         hexadecimal format for the byte array
     */
    public static String toHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {

            String hex = Integer.toHexString(0xFF & bytes[i]);
            if (hex.length() == 1) {
                hexString.append('0');
            }

            hexString.append(hex);
        }

        return hexString.toString();
    }
    
    // //////////////////////////////////////////////////////////////////////////
    // Fields
    // //////////////////////////////////////////////////////////////////////////

    private String fileSeparator = "/"; // file separator

    private String cqlMigrationSeparator = "_"; // script separator

    private Session session; // session

    private String versionSchemaTable = "Version"; // version table name

    private State<String> state;

}
