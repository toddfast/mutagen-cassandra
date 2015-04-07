package mutations.tests.checksum_error.java.d2;

import com.toddfast.mutagen.cassandra.impl.NewCassandraMigrator;

public class M201506060001_Bar_1111 extends NewCassandraMigrator {



    @Override
    protected void performMutation(com.toddfast.mutagen.Mutation.Context context) {
        System.out.println("execting script 2 before");
        try {
            getSession().execute("CREATE TABLE \"Trolololo\" (key varchar PRIMARY KEY,value1 varchar);");

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
        System.out.println("execting script 2");

    }

}
