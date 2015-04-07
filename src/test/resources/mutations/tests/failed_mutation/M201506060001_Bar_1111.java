package mutations.tests.failed_mutation;

import com.toddfast.mutagen.cassandra.impl.NewCassandraMigrator;

// First script, successful
public class M201506060001_Bar_1111 extends NewCassandraMigrator {

    @Override
    protected void performMutation(com.toddfast.mutagen.Mutation.Context context) {

        getSession().execute("CREATE TABLE \"Test1\" (key varchar PRIMARY KEY,value1 varchar);");

    }

}
