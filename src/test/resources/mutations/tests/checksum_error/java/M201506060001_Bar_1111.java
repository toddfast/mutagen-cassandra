package mutations.tests.checksum_error.java;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.cassandra.impl.NewCassandraMigrator;

public class M201506060001_Bar_1111 extends NewCassandraMigrator {

    public M201506060001_Bar_1111() {
        super();
        // TODO Auto-generated constructor stub
    }

    public M201506060001_Bar_1111(Session session) {
        super(session);
        // TODO Auto-generated constructor stub
    }

    @Override
    protected void migrate() {
        getSession().execute("CREATE TABLE \"Trolololo2\" (key varchar PRIMARY KEY,value1 varchar);");

    }

}
