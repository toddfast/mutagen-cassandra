package mutations.tests.execution;


import com.datastax.driver.core.Session;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.cassandra.impl.NewCassandraMigrator;

/**
 *
 * It is a script file java that update table Test1.
 */
public class M201502011225_UpdateTableTest_1111 extends NewCassandraMigrator {

    /**
     * Constructor for the test.
     * 
     * @param session
     *            the session to execute cql statements.
     */
    public M201502011225_UpdateTableTest_1111(Session session) {
        super(session);
    }



    /**
     * Return a canonical representative of the change in string form
     *
     */
    @Override
    protected String getChangeSummary() {
        return "update \"Test1\" set value1='chicken', value2='sneeze' " +
                "where key='row2';";
    }

    @Override
    protected void performMutation(Context context) {
        context.debug("Executing mutation {}", getResultingState().getID());

        try {
            String updateStatement = "update \"Test1\" set value1='chicken', value2='sneeze' " +
                    "where key='row2';";
            getSession().execute(updateStatement);
        } catch (Exception e) {
            throw new MutagenException("Could not update columnfamily Test1", e);
        }
    }


}
