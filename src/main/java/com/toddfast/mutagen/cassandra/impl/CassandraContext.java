package com.toddfast.mutagen.cassandra.impl;

import com.toddfast.mutagen.Coordinator;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the basic contract of {@link Mutation.Context}. Logs to SLF4J.
 * 
 * @author Todd Fast
 */
public class CassandraContext implements Mutation.Context {

	/**
	 *
	 *
	 */
	public CassandraContext(Subject<?> subject, Coordinator<?> coordinator) {
		this(subject,coordinator,
			LoggerFactory.getLogger(CassandraContext.class));
	}


	/**
	 *
	 *
	 */
	public CassandraContext(Subject<?> subject, Coordinator<?> coordinator,
			Logger logger) {
		super();
		this.subject=subject;
		this.coordinator=coordinator;
		this.logger=logger;
	}


	/**
	 *
	 *
	 */
	@Override
	public Subject<?> getSubject() {
		return subject;
	}


	/**
	 *
	 *
	 */
	@Override
	public Coordinator<?> getCoordinator() {
		return coordinator;
	}


	/**
	 *
	 *
	 */
	@Override
	public void info(String message, Object... parameters) {
		logger.info(message,parameters);
	}


	/**
	 *
	 *
	 */
	@Override
	public void debug(String message, Object... parameters) {
		logger.debug(message,parameters);
	}


	/**
	 *
	 *
	 */
	@Override
	public void error(String message, Object... parameters) {
		logger.error(message,parameters);
	}




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	private Subject<?> subject;
	private Coordinator<?> coordinator;
	private Logger logger;
}
