package com.toddfast.mutagen.cassandra.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.toddfast.mutagen.Coordinator;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.Subject;

/**
 * Implements the basic contract of {@link Mutation.Context}. Logs to SLF4J.
 * 
 */
public class CassandraContext implements Mutation.Context {

    /**
     * constructor for CassandraContext.
     * 
     * @param subject
     *            cassandra subject.
     * @param coordinator
     *            cassandra coordinator.
     *
     */
    public CassandraContext(Subject<?> subject, Coordinator<?> coordinator) {
        this(subject, coordinator,
                LoggerFactory.getLogger(CassandraContext.class));
    }

    /**
     * another constructor for CassandraContext with a logger indicated.
     * 
     * @param subject
     *            the cassandra subject
     * @param coordinator
     *            the cassandra coordinator
     * @param logger
     *            the logger to print infomation
     */
    public CassandraContext(Subject<?> subject, Coordinator<?> coordinator,
            Logger logger) {
        super();
        this.subject = subject;
        this.coordinator = coordinator;
        this.logger = logger;
    }

    /**
     * a getter method to get subject.
     *
     * @return subject
     */
    @Override
    public Subject<?> getSubject() {
        return subject;
    }

    /**
     * a getter method to get coordinator.
     *
     * @return coordinator
     */
    @Override
    public Coordinator<?> getCoordinator() {
        return coordinator;
    }

    /**
     * print the important information in the console.
     *
     */
    @Override
    public void info(String message, Object... parameters) {
        logger.info(message, parameters);
    }

    /**
     * print the debug information in the console.
     *
     */
    @Override
    public void debug(String message, Object... parameters) {
        logger.debug(message, parameters);
    }

    /**
     * print the error information in the console.
     *
     */
    @Override
    public void error(String message, Object... parameters) {
        logger.error(message, parameters);
    }

    // //////////////////////////////////////////////////////////////////////////
    // Fields
    // //////////////////////////////////////////////////////////////////////////

    private Subject<?> subject;

    private Coordinator<?> coordinator;

    private Logger logger;
}
