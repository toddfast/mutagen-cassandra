package cassandra.mutations;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.basic.SimpleState;
import com.toddfast.mutagen.cassandra.AbstractCassandraMutation;

/**
 *
 * @author Todd Fast
 */
public class V003 extends AbstractCassandraMutation {

	/**
	 *
	 *
	 */
	public V003(Keyspace keyspace) {
		super(keyspace);
		state=new SimpleState<Integer>(3);
	}


	@Override
	public State<Integer> getResultingState() {
		return state;
	}



	/**
	 * Return a canonical representative of the change in string form
	 *
	 */
	@Override
	protected String getChangeSummary() {
		return "update 'Test1' set value1='chicken', value2='sneeze' "+
			"where key='row2';";
	}

	@Override
	protected void performMutation(Context context) {
		context.debug("Executing mutation {}",state.getID());
		final ColumnFamily<String,String> CF_TEST1=
			ColumnFamily.newColumnFamily("Test1",
				StringSerializer.get(),StringSerializer.get());

		MutationBatch batch=getKeyspace().prepareMutationBatch();
		batch.withRow(CF_TEST1,"row2")
			.putColumn("value1","chicken")
			.putColumn("value2","sneeze");

		try {
			batch.execute();
		}
		catch (ConnectionException e) {
			throw new MutagenException("Could not update columnfamily Test1",e);
		}
	}


	final ColumnFamily<String,String> CF_TEST1=
		ColumnFamily.newColumnFamily("Test1",
			StringSerializer.get(),StringSerializer.get());
	private State<Integer> state;
}
