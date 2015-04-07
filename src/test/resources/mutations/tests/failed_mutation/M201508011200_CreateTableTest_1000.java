package mutations.tests.failed_mutation;

import com.toddfast.mutagen.cassandra.impl.NewCassandraMigrator;

// Third script, successful
public class M201508011200_CreateTableTest_1000 extends NewCassandraMigrator {
    @Override
    protected void performMutation(com.toddfast.mutagen.Mutation.Context context) {
        getSession().execute("insert into \"Test1\" (key, value1) values ('row1', 'value1');");
    }
}
