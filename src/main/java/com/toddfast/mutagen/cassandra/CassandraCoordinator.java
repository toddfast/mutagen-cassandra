package com.toddfast.mutagen.cassandra;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.Coordinator;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.Subject;

/**
 * 
 * 
 * @author Todd Fast
 */
public class CassandraCoordinator implements Coordinator<String> {

    /**
	 * 
	 * 
	 */
    public CassandraCoordinator(Session session) {
        super();
        if (session == null) {
            throw new IllegalArgumentException(
                    "Parameter \"session\" cannot be null");
        }

        this.session = session;
    }

    /**
	 *
	 *
	 */
    public Session getSession() {
        return session;
    }

    /**
	 * 
	 * 
	 */

    @Override
    public boolean accept(Subject<String> subject,
            State<String> targetState) {
        State<String> currentState = subject.getCurrentState();
        System.out.println("currentstate:" + currentState.getID());
        return (targetState.getID().compareTo(currentState.getID()) > 0);
    }

    // //////////////////////////////////////////////////////////////////////////
    // Fields
    // //////////////////////////////////////////////////////////////////////////

    private Session session;

}
