package mutations.tests.simple.java;

import com.toddfast.mutagen.cassandra.impl.NewCassandraMigrator;

public class M201502011200_CreateTableTest_1000 extends NewCassandraMigrator {
    @Override
    protected void performMutation(com.toddfast.mutagen.Mutation.Context context) {
        getSession().execute("CREATE TABLE \"Test1\" (key varchar PRIMARY KEY,value1 varchar);");
        getSession().execute("insert into \"Test1\" (key, value1) values ('row1', 'value1');");
    }
}
