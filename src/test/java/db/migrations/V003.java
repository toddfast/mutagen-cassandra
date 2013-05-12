package db.migrations;

import com.netflix.astyanax.Keyspace;
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


	@Override
	protected void performMutation(Context context) {
		context.debug("Executing mutation {}",state.getID());
	}


	private State<Integer> state;
}
