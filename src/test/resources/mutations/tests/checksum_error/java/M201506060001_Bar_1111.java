package mutations.tests.checksum_error.java;

import com.toddfast.mutagen.cassandra.impl.NewCassandraMigrator;

public class M201506060001_Bar_1111 extends NewCassandraMigrator {

    @Override
    protected void performMutation(com.toddfast.mutagen.Mutation.Context context) {
        getSession().execute("CREATE TABLE \"Trolololo2\" (key varchar PRIMARY KEY,value1 varchar);");

    }

}
