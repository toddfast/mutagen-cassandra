package mutations.tests.wrongJavaScript;


import com.datastax.driver.core.Session;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.basic.SimpleState;
import com.toddfast.mutagen.cassandra.AbstractCassandraMutation;

/**
 *
 * It is a script file java.
 * It is just for test.
 */
public class M201502011225_WrongJavaScriptFile_1111 extends AbstractCassandraMutation {

    /**
     * Constructor for the test.
     * 
     * @param session
     *            the session to execute cql statements.
     */
    public M201502011225_WrongJavaScriptFile_1111(Session session) {
        super(session);
        state = new SimpleState<String>("201502011225");
    }

    @Override
    public State<String> getResultingState() {
        return state;
    }

    /**
     * Return a canonical representative of the change in string form
     *
     */
    @Override
    protected String getChangeSummary() {
        return "create table \"Test1\"();";
    }

    @Override
    protected void performMutation(Context context) {
        context.debug("Executing mutation {}", state.getID());

        try {
            String createTableStatement = "create table \"Test1\"();";
            getSession().execute(createTableStatement);
        } catch (Exception e) {
            throw new MutagenException("Could not create table Test1", e);
        }
    }

    @Override
    protected String getRessourceName() {
        return "M201502011225";
    }

    private State<String> state;
}
