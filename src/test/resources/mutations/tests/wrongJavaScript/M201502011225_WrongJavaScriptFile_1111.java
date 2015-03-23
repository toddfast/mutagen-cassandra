package mutations.tests.wrongJavaScript;


import com.datastax.driver.core.Session;
import com.toddfast.mutagen.cassandra.impl.NewCassandraMigrator;

/**
 *
 * It is a script file java.
 * It is just for test.
 */
public class M201502011225_WrongJavaScriptFile_1111 extends NewCassandraMigrator {

    /**
     * Constructor for the test.
     * 
     * @param session
     *            the session to execute cql statements.
     */
    public M201502011225_WrongJavaScriptFile_1111(Session session) {
        super(session);
    }

    @Override
    protected void migrate() {
        getSession().execute("CREATE TABLE \"Test1\";");
    }
}
