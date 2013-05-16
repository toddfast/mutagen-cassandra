mutagen-cassandra
=================

Mutagen Cassandra is a framework (based on [Mutagen](https://github.com/toddfast/mutagen)) that provides schema versioning and mutation for Apache Cassandra.

Using Mutagen Cassandra
-----------------------

### 1. Create a package for mutations

Create a Java-style package in your project to contain versioned schema mutation files. The package name can be anything, but it should ideally be specific to the schema you will be mutating. (In the same way that Java packages prevent namespace collisions for classes, you don't want to accidentally mix someone else's mutations into your schema by using a common package name.)

### 2. Add mutations

Add mutations. Mutations can be either declarative CQL2/3 or Java classes. In both cases, the root mutation file name should start with a **version tag**--a prefix that orders the files naturally with a zero-padded integer, like `V001`.  (Anything following the version tag is just a comment for your own use; the verion tag ends with the first non-numeric character.)

Examples:

* `V001_Some_cql_update.cql`
* `V002_Some_java_update.java`
* `V003.cql`
* `003.cql` (Note that this convention doesn't work for Java classes, so I recommend using a prefix that is compatible with Java identifier rules.)

Lastly, note that if you use Maven, you'll need to put `.cql` files and `.java` files in separate source roots, but Mutagen will find them and order them properly as long as the version tags are consistent. 

### Invoke Mutagen Cassandra at runtime

At runtime (normally during app startup), get or create an instance of `CassandraMutagen`. (It's easy to use Nu to get an instance by calling `$(CassandraMutagen.class)`. Note that the instance returned by Nu is client-managed, i.e. not a singleton.)

Call `CassandraMutagen.initialize()` and provide the package name containing your mutations. You should see log messages listing all the resources that were found.

Obtain an Astyanax `Keyspace` instance. Mutagen Cassandra use the Netflix Astyanax Cassandra client, and requires a configured `Keyspace` instance to work. This should obviously be straightforward if you already use Astyanax. If not, please see [the documentation on the Astyanax wiki](https://github.com/Netflix/astyanax/wiki/Create-keyspace-or-column-family).

To perform the mutations, call `CassandraMutagen.mutate(Keyspace);` to update the Cassandra schema to the latest version. Please note, **this method may not throw an exception if there is a problem.** Instead, use the returned value of type `Plan.Result<Integer>` to check for any exceptions thrown during the process. This may change in the future, so it would be prudent to surround your call to `mutate()` with a `try…catch` for `MutagenException`.

### Continue adding mutations

As you continue development, continue adding mutation files to the mutations package and running Mutagen Cassandra during startup. There is no need to conditionalize running Mutagen, as its purpose is to check each time for new schema mutations and apply them.
