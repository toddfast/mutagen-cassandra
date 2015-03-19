mutagen-cassandra
=================

Mutagen Cassandra is a framework (based on [Mutagen](https://github.com/toddfast/mutagen)) that provides schema versioning and mutation for Apache Cassandra.

Mutagen is a lightweight framework for applying versioned changes (known as *mutations*) to a resource, in this case a Cassandra schema. Mutagen takes into account the resource's existing state and only applies changes that haven't yet been applied.

Schema mutation with Mutagen helps you make manageable changes to the schema of live Cassandra instances as you update your software, and is especially useful when used across development, test, staging, and production environments to automatically keep schemas in sync.

Getting Started
---------------

### 1. Create a package for mutations

Create a Java-style package in your project to contain versioned schema mutation files. The package name can be anything, but it should ideally be specific to the schema you will be mutating. (In the same way that Java packages prevent namespace collisions for classes, you don't want to accidentally mix someone else's mutations into your schema by using a common package name.)

### 2. Create mutations

Mutations can be either declarative CQL2/3 or Java classes that use whatever Cassandra client you like (Datastax is supported directly).

The root package name should be the same for both, and the mutation file names should start with a **version tag**--a prefix that orders the files naturally with a zero-padded integer.  (Anything following the version tag is just a comment for your own use; the verion tag ends with the first non-numeric character.)

The name convention for mutation:
-M<DATETIME>_<Camel case title>_<ISSUE>.cqlsh.txt
-M<DATETIME>_<Camel case title>_<ISSUE>.java

Examples:
- M201502011200_RemoveStrategyLoad_2627.cqlsh.txt
- M201502011201_RemoveStrategyLoad_2627.java

Lastly,you should put the `.cqlsh.txt` files and `.java` files under the same source roots.Mutagen will compile the java file in .class file. 

### 3. Mutate!

````java
try {
	// Get an instance of CassandraMutagen somehow (ideally inject it)
	CassandraMutagen mutagen = ... // e.g. new CassandraMutagenImpl();

	// Initialize the list of mutations
	mutagen.initialize("my/cassandra/mutations");

	// Get an Datastax session
	Session session = ...

	// Mutate! Note, this method may not throw an exception.
	Plan.Result<Integer> result = mutagen.mutate(session);
	
	// Inspect result, especially for an exception
	if (result.getException() != null) {
		// Throw an exception
	}

	// Did something else go wrong?
	if (!result.isMutationComplete()) {
		// Figure out what happened
	}
}
catch (IOException e) {
	// Problem loading the mutations
}
catch (MutagenException e) {
	// Mutation failed
}
````

At runtime (normally during app startup), get or create an instance of `CassandraMutagen`. You can just create an instance using: `new CassandraMutageImpl()`.

Call `CassandraMutagen.initialize()` and provide the package name containing your mutations. You should see log messages listing all the resources that were found.

Obtain an Datastax `Session` instance. Mutagen Cassandra use the Datastax Cassandra client, and requires a configured `Session` instance to work. This should obviously be straightforward if you already use Datastax. If not, please see the [Session documentation](http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/Session.html).

To perform the mutations, call `CassandraMutagen.mutate(session);` to update the Cassandra schema to the latest version. Please note, **this method may not throw an exception if there is a problem.** Instead, use the returned value of type `Plan.Result<Integer>` to check for any exceptions thrown during the process. This may change in the future, so it would be prudent to surround your call to `mutate()` with a `try...catch` for `MutagenException`.

### 4. Continue adding mutations

As you continue development, continue adding mutation files to the mutations package and running Mutagen Cassandra during startup. There is no need to conditionalize running Mutagen, as its purpose is to check each time for new schema mutations and apply them.

Examples
--------

For examples of using Mutagen Cassandra, see the unit tests. To run the tests, you'll need a running instance of Cassandra running on the default port (9160).

Note that the unit test mixes declarative CQL mutations with a Java mutation (`M201502011225_UpdateTableTest_1111.java`).

PS:in our unit tests, we use the `Achilles`(https://github.com/doanduyhai/Achilles),it comes along with a junit rule to start an embedded cassandra server in memory and bootstrap the framework.It can create the session easily(https://github.com/doanduyhai/Achilles/wiki/Unit-testing).
```
public AchillesResource resource = AchillesResourceBuilder
            .noEntityPackages().withKeyspaceName(keyspace).build();
    public Session session = resource.getNativeSession();
```

Other Details
-------------

### The `schema_version` column family

Mutagen Cassandra adds a column family to the keyspace `apispark` called `Version` which tracks the current version of the schema. It doesn't otherwise change your keyspace in any way (like dropping and recreating it--whoops!), so it's possible to mix and match versioned and non-versioned column families in the same keyspace.

### Using Mutagen with an existing schema

Mutagen *mutates* schemas; it doesn't assume it owns them. If you already have a schema in Cassandra and want to start mutating it with Mutagen, you needn't do anything but use Mutagen as described above (starting with whatever version number you like). It will automatically create the `Version` column family and happily start applying mutations. Mutagen doesn't know or care semantically what the mutations it's applying are; just be sure that mutations targeting existing column familes *alter* them instead of creating them.

### Manual changes to a schema

Although it's best practice to always use Mutagen to mutate your schema, it's possible to make manual modifications to your schema outside of Mutagen. As long as future mutations take these changes into account, Mutagen won't itself have a problem. This capability might come in useful during disaster recovery, for example.

However, it then becomes your responsibility to be sure that all instances of the schema (for example, between dev, test, and production) apply the same manual changes, which is sort of the point of using Mutagen in the first place!

It might instead make sense to create mutations reflecting the manual changes, but then manually update the `version` column (it's an `string`)in the `state` row of the `Version` column family to prevent those mutations from being applied. That way, if you ever recreate the schema, every change will be there.

### CQL mutations

The easiest way to mutate your Cassandra schema is by using declarative CQL statements. Just be aware that Mutagen Cassandra treats all CQL statements in a single mutation file (separated by semicolons) as a single mutation.

The CQL version that you use is governed by the configuration of the Datastax `Session` passed to `CassandraMutagen`. Make sure that you set the CQL version to match the statements you'll be using.

### Undoing mutations

Mutagen Cassandra doesn't support undoing mutations. Once a mutant, always a mutant.

In practice, this means that you need to take the approach of "patching your way to the future". If you made a change in a past mutation that you want to undo, create a new mutation to undo it. Never go back and change existing mutations, as they won't be applied, and worst case they will be applied to another schema instance and things will get horribly out of sync. You've been warned.

### Clustered environments

Recent versions of Cassandra handle schema changes in clustered deployments just fine, so no problems there.

However, the current version of Mutagen Cassandra only provides VM-wide synchronization for mutations. This means that if you have multiple client nodes that embed Mutagen Cassandra, you will need to provide your own external coordination to ensure that only a single node is updating the schema with Mutagen at any given time.

In practice, if you're already dealing with a clustered environment, this should be relatively straightforward. For example, you could use ZooKeeper, a file on an external Web server (or S3), Quartz, a queue with reservations, or something else that has reasonable semantics for coordinating multiple nodes. (If you are running a clustered environment and don't know what any of those mean, you probably have bigger fish to fry.)

The proper thing is for Mutagen Cassandra to handle this coordination via a cluster-aware `Coordinator` implementation (ideally using Cassandra itself), but that is an upcoming feature and not yet available.