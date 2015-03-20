// /**
// * Copyright 2010-2014 Restlet S.A.S. All rights reserved.
// *
// * Restlet and APISpark are registered trademarks of Restlet S.A.S.
// */
//
// package com.toddfast.mutagen.cassandra.impl;
//
// import java.io.BufferedReader;
// import java.io.IOException;
// import java.io.InputStreamReader;
// import java.io.PrintStream;
// import java.util.Arrays;
// import java.util.HashMap;
// import java.util.Map;
//
// import com.apispark.db.MainUtils;
// import com.datastax.driver.core.BoundStatement;
// import com.datastax.driver.core.Cluster;
// import com.datastax.driver.core.PreparedStatement;
// import com.datastax.driver.core.ResultSet;
// import com.datastax.driver.core.Session;
// import com.restlet.store.cassandra.utils.DatastaxDriverUtils;
// import com.restlet.store.cassandra.utils.PropertiesUtils;
// import com.toddfast.mutagen.MutagenException;
// import com.toddfast.mutagen.cassandra.AbstractCassandraMutation;
//
// /**
// * Base classe for cassandra migration tool.
// *
// * @author thboileau
// *
// */
// public abstract class NewCassandraMigrator extends AbstractCassandraMutation{
//
//
// public static final String KEYSPACE_APISPARK = "apispark";
//
// public static final String DEFAULT_QUERY_LIMIT = " limit " + 1_000_000_000;
//
// public static final int DEFAULT_FETCH_SIZE = 1000;
//
// private boolean manualRun = false;
//
// private boolean dry;
//
// // Used only for manual execution.
// private Cluster cluster;
// private Session scriptOnlySession;
//
// private String keyspace;
//
// /**
// * Empty constructor used only for test class
// */
// public NewCassandraMigrator() {
// super(null);
// }
//
// protected NewCassandraMigrator(Session session) {
// super(session);
// // TODO Auto-generated constructor stub
// }
// /**
// * Run the migration script with given program arguments. <br>
// * Run with <code>-h</code> argument to display help. <br>
// * Run with <code>--dry</code> argument to print only the statements without
// * executing them.
// *
// * @param args
// * The program arguments
// */
// public void run(String[] args) {
// manualRun = true;
// initialize(KEYSPACE_APISPARK, args);
// execute();
// }
//
// private void initialize(String keyspace, String[] args) {
// this.keyspace = keyspace;
// String propertiesPath = System.getProperty("cassandra.properties.file");
// if (propertiesPath == null) {
// throw new RuntimeException(
// "System property 'cassandra.properties.file' required.");
// }
// // TODO seems useless, no?
// Map<String, String> map = new HashMap<String, String>();
// PropertiesUtils.loadProperties(propertiesPath, map);
//
// for (String arg : args) {
// if ("--dry".equals(arg)) {
// dry = true;
// } else if ("-h".equals(arg) || "--help".equals(arg)) {
// printHelp();
// System.exit(0);
// } else {
// System.out.println("Unknown option: " + arg);
// printHelp();
// System.exit(1);
// }
// }
// }
//
// public final void execute() {
// // Create getSession() to hosts
// if(manualRun){
// cluster = DatastaxDriverUtils.createCluster();
// setScriptOnlySession(DatastaxDriverUtils.createSession(cluster, keyspace));
// }
//
// try {
// // Initialize output formatting strategy
// migrate();
// } finally {
// if(manualRun){
// getSession().close();
// cluster.close();
// }
// }
// }
//
// /**
// * Override to add migration code.
// */
// protected abstract void migrate();
//
// // Called only by the mutagen framework
// @Override
// protected void performMutation(Context context) {
// try {
//
// migrate();
// } catch (Exception e) {
// throw new MutagenException("Exception executing Java script : \""+getRessourceName()+"\"",e);
//
// }
// }
// /**
// * Affiche l'aide de la commande.
// *
// * @throws IOException
// */
// private void printHelp() {
// PrintStream o = System.out;
//
// o.println("SYNOPSIS");
// MainUtils.printSynopsis(o, getClass(), "[options]");
// o.println("DESCRIPTION");
// MainUtils.printSentence(o, "Migration for APISpark 3.3.0");
// o.println("OPTIONS");
// MainUtils.printOption(o, "-h", "Prints this help");
// MainUtils.printOption(o, "-stat",
// "if set, just display the list of migration instructions");
// }
//
// /**
// * Executes a shell command or prints a trace.
// *
// * @param command
// * The command to execute.
// */
// public void executeShellCommand(String command) {
// if (dry) {
// System.out.println(command);
// } else {
// String s = null;
// try {
// Process p = Runtime.getRuntime().exec(
// new String[] { "sh", "-c", command });
//
// BufferedReader stdInput = new BufferedReader(
// new InputStreamReader(p.getInputStream()));
//
// BufferedReader stdError = new BufferedReader(
// new InputStreamReader(p.getErrorStream()));
//
// // read the output from the command
// while ((s = stdInput.readLine()) != null) {
// System.out.println(s);
// }
//
// // read any errors from the attempted command
// while ((s = stdError.readLine()) != null) {
// System.err.println(s);
// }
// } catch (IOException e) {
// System.out.println("exception happened - here's what I know: ");
// e.printStackTrace();
// }
// }
// }
//
// /**
// * Executes a curl command or prints a trace.
// *
// * @param command
// * The command to execute.
// */
// public String executeCurlCommand(String command) {
// String s = "";
// StringBuffer sb = new StringBuffer();
//
// if (dry) {
// System.out.println(command);
// } else {
// try {
// Process p = Runtime.getRuntime().exec(
// new String[] { "sh", "-c", command });
//
// BufferedReader stdInput = new BufferedReader(
// new InputStreamReader(p.getInputStream()));
//
// // read the output from the command
// while ((s = stdInput.readLine()) != null) {
// sb.append(s);
// }
// } catch (IOException e) {
// System.out.println("exception happened - here's what I know: ");
// e.printStackTrace();
// }
// }
// return sb.toString();
// }
//
// /**
// * Executes an update request or prints a trace.
// *
// * @param statement
// * The statement to execute.
// * @param values
// * The values.
// */
// public void executeUpdate(String statement, Object... values) {
// executeUpdate(getSession().prepare(statement), values);
// }
//
// /**
// * Prepares a statement.
// *
// * @param request
// * The request to prepare.
// */
// public PreparedStatement prepare(String request) {
// return getSession().prepare(request);
// }
//
// /**
// * Executes an update request or prints a trace.
// *
// * @param statement
// * The statement to execute.
// * @param values
// * The values.
// */
// public void executeUpdate(PreparedStatement statement, Object... values) {
// if (dry) {
// System.out.println(statement.getQueryString() + " << "
// + Arrays.asList(values));
// } else {
// BoundStatement boundStatement = statement.bind(values);
// getSession().execute(boundStatement);
// }
// }
//
// /**
// * Execute a request or prints a trace.
// *
// * @param request
// * The request to execute.
// * @return
// */
// public ResultSet query(String statement, Object... values) {
// if (!statement.startsWith("select")) {
// throw new RuntimeException(
// "A query request should starts with select statement.");
// }
// PreparedStatement psu = getSession().prepare(statement);
// BoundStatement boundStatement = psu.bind(values);
// boundStatement.setFetchSize(DEFAULT_FETCH_SIZE);
// return getSession().execute(boundStatement);
// }
//
// public Session getSession() {
// if(manualRun)
// return scriptOnlySession;
// else
// return super.getSession();
// }
//
// public void setScriptOnlySession(Session scriptOnlySession) {
// this.scriptOnlySession = scriptOnlySession;
// }
// }
