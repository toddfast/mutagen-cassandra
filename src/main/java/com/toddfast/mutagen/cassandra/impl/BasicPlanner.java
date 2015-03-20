package com.toddfast.mutagen.cassandra.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.toddfast.mutagen.Coordinator;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.Mutation.Context;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.Planner;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.Subject;
import com.toddfast.mutagen.basic.BasicContext;

/**
 * Generates basic plans using the initial list of mutations and the specified
 * subject and coordinator. The list of mutations is cloned and cannot be
 * modified after creation.
 * 
 * @author Todd Fast
 */
public class BasicPlanner<I extends Comparable<I>> implements Planner<I>  {

	/**
	 *
	 *
	 */
	public BasicPlanner(Collection<Mutation<I>> allMutations) {
		this(allMutations,null);
	}


	/**
	 *
	 *
	 */
	public BasicPlanner(Collection<Mutation<I>> allMutations,
			Comparator<Mutation<I>> comparator) {
		super();
		this.mutations=new ArrayList<Mutation<I>>(allMutations);
		if (comparator!=null) {
			Collections.sort(this.mutations,comparator);
		}
	}


	/**
	 *
	 *
	 */
	@Override
	public Plan<I> getPlan(Subject<I> subject, Coordinator<I> coordinator) {

		List<Mutation<I>> subjectMutations=
			new ArrayList<Mutation<I>>(mutations);

		// Filter out the mutations that are unacceptable to the subject
		for (Iterator<Mutation<I>> i=subjectMutations.iterator();
				i.hasNext(); ) {

			Mutation<I> mutation=i.next();
			if (!coordinator.accept(subject,mutation.getResultingState())) {
				i.remove();
			}
		}

		return new BasicPlan(subject,coordinator,subjectMutations);
	}


	/**
	 *
	 *
	 */
	private BasicResult executePlan(BasicPlan plan) {

		List<Mutation<I>> completedMutations=new ArrayList<Mutation<I>>();
		List<Mutation<I>> remainingMutations=
			new ArrayList<Mutation<I>>(plan.getMutations());
		MutagenException exception=null;

		Context context=createContext(
			plan.getSubject(),plan.getCoordinator());

		Mutation<I> mutation;
		State<I> lastState=null;
		for (Iterator<Mutation<I>>
				i=remainingMutations.iterator(); i.hasNext(); ) {

			mutation=i.next();

			try {
				mutation.mutate(context);

				lastState=mutation.getResultingState();

				// Add to the completed list, remove from remaining list
				completedMutations.add(mutation);
				i.remove();
			}
			catch (RuntimeException e) {
				exception=new MutagenException("Exception executing "+
					"mutation for state \""+mutation.getResultingState()+
					"\"",e);
				break;
			}
		}

		return new BasicResult(plan,plan.getSubject(),
			completedMutations,remainingMutations,lastState,exception);
	}


	/**
	 *
	 *
	 */
	protected Context createContext(Subject<I> subject,
			Coordinator<I> coordinator) {
		return new BasicContext(subject,coordinator);
	}

        public List<Mutation<I>> getMutations() {
            return mutations;
        }



	////////////////////////////////////////////////////////////////////////////
	// Types
	////////////////////////////////////////////////////////////////////////////

	/**
	 *
	 *
	 */
	public class BasicPlan implements com.toddfast.mutagen.Plan<I> {

		/**
		 *
		 *
		 */
        public BasicPlan(Subject<I> subject, Coordinator<I> coordinator,
				List<Mutation<I>> mutations) {
			super();
			this.subject=subject;
			this.coordinator=coordinator;
			this.mutations=mutations;
		}

		/**
		 *
		 *
		 */
		@Override
		public Subject<I> getSubject() {
			return subject;
		}

		/**
		 *
		 *
		 */
		@Override
		public Coordinator<I> getCoordinator() {
			return coordinator;
		}

		/**
		 *
		 *
		 */
		@Override
		public List<Mutation<I>> getMutations() {
			return Collections.unmodifiableList(mutations);
		}

		/**
		 *
		 *
		 */
		@Override
		public Result<I> execute()
				throws MutagenException {
			return BasicPlanner.this.executePlan(this);
		}

		private Subject<I> subject;
		private Coordinator<I> coordinator;
		private List<Mutation<I>> mutations;
	}




	////////////////////////////////////////////////////////////////////////////
	// Types
	////////////////////////////////////////////////////////////////////////////

	/**
	 *
	 *
	 */
	public class BasicResult implements com.toddfast.mutagen.Plan.Result<I> {

		/**
		 *
		 *
		 */
		private BasicResult(BasicPlan plan,
				Subject<I> subject,
				List<Mutation<I>> completedMutations,
				List<Mutation<I>> remainingMutations,
				State<I> lastState,
				MutagenException exception) {
			super();
			this.plan=plan;
			this.subject=subject;
			this.completedMutations=completedMutations;
			this.remainingMutations=remainingMutations;
			this.lastState=lastState;
			this.exception=exception;
		}

		/**
		 *
		 *
		 */
		@Override
		public com.toddfast.mutagen.Plan<I> getPlan() {
			return plan;
		}

		/**
		 *
		 *
		 */
		@Override
		public boolean isMutationComplete() {
			return remainingMutations.isEmpty();
		}


		/**
		 *
		 *
		 */
		@Override
		public State<I> getLastState() {
			return lastState;
		}


		/**
		 *
		 *
		 */
		@Override
		public List<Mutation<I>> getCompletedMutations() {
			return completedMutations;
		}


		/**
		 *
		 *
		 */
		@Override
		public List<Mutation<I>> getRemainingMutations() {
			return remainingMutations;
		}


		/**
		 *
		 *
		 */
		@Override
		public MutagenException getException() {
			return exception;
		}


		private BasicPlan plan;
		private Subject<I> subject;
		private List<Mutation<I>> completedMutations;
		private List<Mutation<I>> remainingMutations;
		private State<I> lastState;
		private MutagenException exception;
	}




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	public static final Comparator<Mutation<?>> COMPARATOR=
		new Comparator<Mutation<?>>() {
			@Override
			public int compare(Mutation m1, Mutation m2) {
				return m1.getResultingState().compareTo(m2.getResultingState());
			}
		};

	private List<Mutation<I>> mutations;
}
