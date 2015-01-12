/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.procedure2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.procedure2.ProcedureResult;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.procedure2.util.TimeoutBlockingQueue;
import org.apache.hadoop.hbase.procedure2.util.TimeoutBlockingQueue.TimeoutRetriever;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureState;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Thread Pool that executes the submitted procedures.
 * The executor has a ProcedureStore associated.
 * Each operation is logged and on restart the pending procedures are resumed.
 *
 * Unless the Procedure code throws an error (e.g. invalid user input)
 * the procedure will complete (at some point in time), On restart the pending
 * procedures are resumed and the once failed will be rolledback.
 *
 * The user can add procedures to the executor via submitProcedure(proc)
 * check for the finished state via isFinished(procId)
 * and get the result via getResult(procId)
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProcedureExecutor<TEnvironment> {
  private static final Log LOG = LogFactory.getLog(ProcedureExecutor.class);

  Testing testing = null;
  public static class Testing {
    public boolean killBeforeStoreUpdate = false;
  }

  /**
   * Used by the TimeoutBlockingQueue to get the timeout interval of the procedure
   */
  private static class ProcedureTimeoutRetriever implements TimeoutRetriever<Procedure> {
    @Override
    public long getTimeout(Procedure proc) {
      long delta = (EnvironmentEdgeManager.currentTime() - proc.getLastUpdate());
      return Math.max(0, proc.getTimeout() - delta);
    }

    @Override
    public TimeUnit getTimeUnit(Procedure proc) {
      return TimeUnit.MILLISECONDS;
    }
  }

  /**
   * Internal cleaner that removes the completed procedure results after a TTL.
   *
   * Since the client code looks more or less like:
   *   procId = master.doOperation()
   *   while (master.getProcResult(procId) == ProcInProgress);
   * The master should not throw away the proc result as soon as the procedure is done
   * but should wait a result request from the client (see executor.remove(procId))
   * The client will call something like master.isProcDone() or master.getProcResult()
   * which will return the result/state to the client, and it will mark the completed
   * proc as ready to delete. note that the client may not receive the response from
   * the master (e.g. master failover) so, if we delay a bit the real deletion of
   * the proc result the client will be able to get the result the next try.
   */
  private static class CompletedProcedureCleaner<TEnvironment>
      extends PeriodicProcedure<TEnvironment> {
    private static final Log LOG = LogFactory.getLog(CompletedProcedureCleaner.class);

    private static final long EVICT_TTL = 5 * 60000; // 5min
    private static final int TIMEOUT = 5000; // 5sec

    private final Map<Long, ProcedureResult> completed;
    private final ProcedureStore store;

    public CompletedProcedureCleaner(final ProcedureStore store,
        final Map<Long, ProcedureResult> completedMap) {
      super(TIMEOUT);
      this.completed = completedMap;
      this.store = store;
    }

    @Override
    public void periodicExecute(final TEnvironment env) {
      LOG.debug("processing " + completed.size() + " completed procedures");
      long now = System.currentTimeMillis();
      Iterator<Map.Entry<Long, ProcedureResult>> it = completed.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<Long, ProcedureResult> entry = it.next();
        // TODO: Select TTL based on Procedure type
        if ((now - entry.getValue().getTimestamp()) > EVICT_TTL) {
          LOG.debug("Evict completed procedure " + entry.getKey());
          store.delete(entry.getKey());
        }
      }
    }
  }

  private final AtomicInteger activeExecutorCount = new AtomicInteger(0);
  private final AtomicBoolean running = new AtomicBoolean(false);

  private final ConcurrentHashMap<Long, ProcedureResult> completed =
    new ConcurrentHashMap<Long, ProcedureResult>();
  private final ConcurrentHashMap<Long, ProcedureStack> rollbackStack =
    new ConcurrentHashMap<Long, ProcedureStack>();
  private final ConcurrentHashMap<Long, Procedure> procedures =
    new ConcurrentHashMap<Long, Procedure>();
  private final TimeoutBlockingQueue<Procedure> waitingTimeout =
    new TimeoutBlockingQueue<Procedure>(new ProcedureTimeoutRetriever());

  private final AtomicLong lastProcId = new AtomicLong(-1);
  private final ProcedureRunnableSet runnables;
  private final ProcedureStore store;
  private final TEnvironment environment;

  private Thread[] threads;

  public ProcedureExecutor(final TEnvironment environment, final ProcedureStore store) {
    this(environment, store, new ProcedureSimpleRunQueue());
  }

  public ProcedureExecutor(final TEnvironment environment, final ProcedureStore store,
      final ProcedureRunnableSet runqueue) {
    this.environment = environment;
    this.runnables = runqueue;
    this.store = store;
  }

  private List<Map.Entry<Long, ProcedureStack>> load() throws IOException {
    assert completed.isEmpty();
    assert rollbackStack.isEmpty();
    assert procedures.isEmpty();
    assert waitingTimeout.isEmpty();
    assert runnables.size() == 0;

    // 1. Load the procedures
    Iterator<Procedure> loader = store.load();
    if (loader == null) {
      lastProcId.set(0);
      return null;
    }

    long logMaxProcId = 0;
    int runnablesCount = 0;
    while (loader.hasNext()) {
      Procedure proc = loader.next();
      procedures.put(proc.getProcId(), proc);
      logMaxProcId = Math.max(logMaxProcId, proc.getProcId());
      LOG.debug("Loading procedure " + proc);
      if (!proc.hasParent() && !proc.isSuccess()) {
        rollbackStack.put(proc.getProcId(), new ProcedureStack());
      }
      if (proc.getState() == ProcedureState.RUNNABLE) {
        runnablesCount++;
      }
    }
    assert lastProcId.get() < 0;
    lastProcId.set(logMaxProcId + 1);

    // 2. Initialize the stacks
    HashSet<Procedure> runnableSet = null;
    HashSet<Procedure> waitingSet = null;
    for (final Procedure proc: procedures.values()) {
      Long rootProcId = getRootProcedureId(proc);
      if (rootProcId == null) {
        // The 'proc' was ready to run but the root procedure was rolledback?
        runnables.addBack(proc);
        continue;
      }

      if (!proc.hasParent() && proc.isSuccess()) {
        LOG.debug("The procedure is completed " + proc + " -> " +
                  proc.getState() + " " + proc.hasException());
        assert !rollbackStack.contains(proc.getProcId());
        completed.put(proc.getProcId(), newResultFromProcedure(proc));
        continue;
      }

      if (proc.hasParent() && !proc.isFinished()) {
        Procedure parent = procedures.get(proc.getParentProcId());
        // corrupted procedures are handled later at step 3
        if (parent != null) {
          parent.incChildrenLatch();
        }
      }

      ProcedureStack procStack = rollbackStack.get(rootProcId);
      procStack.loadStack(proc);

      switch (proc.getState()) {
        case RUNNABLE:
          if (runnableSet == null) {
            runnableSet = new HashSet<Procedure>(runnablesCount);
          }
          runnableSet.add(proc);
          break;
        case WAITING_TIMEOUT:
          if (waitingSet == null) {
            waitingSet = new HashSet<Procedure>();
          }
          waitingSet.add(proc);
          break;
        case FINISHED:
          if (proc.hasException()) {
            // add the proc to rollback
            runnables.addBack(proc);
            break;
          }
        case ROLLEDBACK:
        case INITIALIZING:
          String msg = "Unexpected " + proc.getState() + " state for " + proc;
          LOG.error(msg);
          throw new UnsupportedOperationException(msg);
      }
    }

    // 3. Validate the stacks
    List<Map.Entry<Long, ProcedureStack>> corrupted = null;
    Iterator<Map.Entry<Long, ProcedureStack>> itStack = rollbackStack.entrySet().iterator();
    while (itStack.hasNext()) {
      Map.Entry<Long, ProcedureStack> entry = itStack.next();
      ProcedureStack procStack = entry.getValue();
      if (procStack.isValid()) continue;

      for (Procedure proc: procStack.getSubprocedures()) {
        procedures.remove(proc.getProcId());
        if (runnableSet != null) runnableSet.remove(proc);
        if (waitingSet != null) waitingSet.remove(proc);
      }
      itStack.remove();
      if (corrupted == null) {
        corrupted = new ArrayList<Map.Entry<Long, ProcedureStack>>();
      }
      corrupted.add(entry);
    }

    // 4. Push the runnables
    if (runnableSet != null) {
      for (Procedure proc: runnableSet) {
        runnables.addBack(proc);
      }
    }
    return corrupted;
  }

  public void start(int numThreads) throws IOException {
    if (running.getAndSet(true)) {
      LOG.warn("Already running");
      return;
    }

    // We have numThreads executor + one timer thread used for timing out
    // procedures and triggering periodic procedures.
    threads = new Thread[numThreads + 1];
    LOG.info("Starting " + threads.length + " procedure executors");

    // Initialize procedures executor
    for (int i = 0; i < numThreads; ++i) {
      threads[i] = new Thread() {
        @Override
        public void run() {
          execLoop();
        }
      };
    }

    // Initialize procedures timeout handler (this is the +1 thread)
    threads[numThreads] = new Thread() {
      @Override
      public void run() {
        timeoutLoop();
      }
    };

    // TODO: Split in two steps.
    // The first one will make sure that we have the latest id,
    // so we can start the threads and accept new procedures.
    // The second step will do the actual load of old procedures.
    load();

    // Start the executors. Here we must have the lastProcId set.
    for (int i = 0; i < threads.length; ++i) {
      threads[i].start();
    }

    // Add completed cleaner
    waitingTimeout.add(new CompletedProcedureCleaner(store, completed));
  }

  public void stop() {
    LOG.info("Stopping the procedure executor");
    running.set(false);
    runnables.signalAll();
    waitingTimeout.signalAll();
  }

  public boolean isRunning() {
    return running.get();
  }

  public int getNumThreads() {
    return threads == null ? 0 : threads.length;
  }

  public int getActiveExecutorCount() {
    return activeExecutorCount.get();
  }

  public TEnvironment getEnvironment() {
    return this.environment;
  }

  /**
   * Add a new procedure to the executor.
   * @param proc the new procedure to execute.
   * @return the procedure id, that can be used to monitor the operation
   */
  public long submitProcedure(final Procedure proc) {
    assert proc.getState() == ProcedureState.INITIALIZING;
    assert isRunning() && lastProcId.get() >= 0;

    // Initialize the Procedure ID
    proc.setProcId(lastProcId.incrementAndGet());

    // Commit the transaction
    store.insert(proc, null);

    // Create the rollback stack for the procedure
    ProcedureStack stack = new ProcedureStack();
    rollbackStack.put(proc.getProcId(), stack);

    // Submit the new subprocedures
    assert !procedures.containsKey(proc.getProcId());
    procedures.put(proc.getProcId(), proc);
    runnables.addBack(proc);
    return proc.getProcId();
  }

  public Map<Long, ProcedureResult> getResults() {
    return Collections.unmodifiableMap(completed);
  }

  public Procedure getProcedure(final long procId) {
    return procedures.get(procId);
  }

  public ProcedureResult getResult(final long procId) {
    return completed.get(procId);
  }

  public boolean isFinished(final long procId) {
    return completed.containsKey(procId);
  }

  public void remove(final long procId) {
    ProcedureResult result = completed.get(procId);
    // TODO: at the moment the TTL cleaner will run and remove this procedure
    // since we have an explicit request we can perform an explicit delete.
  }

  public boolean abort(final long procId) {
    Procedure proc = procedures.get(procId);
    if (proc != null) {
      proc.abort(getEnvironment());
      return true;
    }
    return false;
  }

  private void execLoop() {
    while (running.get()) {
      Long procId = runnables.poll();
      Procedure proc = procId != null ? procedures.get(procId) : null;
      if (proc == null) continue;

      try {
        activeExecutorCount.incrementAndGet();
        execLoop(proc);
      } finally {
        activeExecutorCount.decrementAndGet();
      }
    }
  }

  private void execLoop(Procedure proc) {
    Long rootProcId = getRootProcedureId(proc);
    if (rootProcId == null) {
      // The 'proc' was ready to run but the root procedure was rolledback
      executeRollback(proc);
      return;
    }

    ProcedureStack procStack = rollbackStack.get(rootProcId);
    if (procStack == null) return;

    do {
      // Try to acquire the execution
      if (!procStack.acquire(proc)) {
        if (procStack.setRollback()) {
          // we have the 'rollback-lock' we can start rollingback
          executeRollback(rootProcId, procStack);
        } else {
          // if we can't rollback means that some child is still running.
          // the rollback will be executed after all the children are done.
          // If the procedure was never executed, remove and mark it as rolledback.
          if (proc != null && !proc.wasExecuted()) {
            executeRollback(proc);
          }
        }
        break;
      }

      // Execute the procedure
      assert proc.getState() == ProcedureState.RUNNABLE;
      if (proc.acquireLock(getEnvironment())) {
        execProcedure(procStack, proc);
        proc.releaseLock(getEnvironment());
      } else {
        runnables.yield(proc);
      }
      procStack.release(proc);

      // allows to kill the executor before something is stored to the wal.
      // useful to test the procedure recovery.
      if (testing != null && testing.killBeforeStoreUpdate) {
        break;
      }

      if (proc.getProcId() == rootProcId && proc.isSuccess()) {
        // Finalize the procedure state
        completed.put(rootProcId, newResultFromProcedure(proc));
        rollbackStack.remove(rootProcId);
        LOG.info("Procedure completed: " + proc);
        break;
      }
    } while (procStack.isFailed());
  }

  private void timeoutLoop() {
    while (running.get()) {
      Procedure proc = waitingTimeout.poll();
      if (proc == null) continue;
      if (proc.getMillisToTimeout() > 100) {
        // got an early wake, maybe a stop?
        // re-enqueue the task in case was not a stop or just a signal
        waitingTimeout.add(proc);
        continue;
      }

      // TODO-MAYBE: Should we provide a notification to the store with the
      // full set of procedures pending and completed to write a compacted
      // version of the log (in case is a log)?
      // In theory no, procedures are have a short life, so at some point the store
      // will have the tracker saying everything is in the last log.

      // PeriodicProcedure are not real procedures they are more like java Timers
      if (proc instanceof PeriodicProcedure) {
        if (proc.acquireLock(getEnvironment())) {
          // TODO: if the periodic procedure end up being useful,
          // this should just push the procedure in the runnables queue.
          // At the moment the only user is the CompletedProcedureCleaner.
          proc.doExecute(getEnvironment());
          proc.releaseLock(getEnvironment());
          if (!proc.isFinished()) {
            waitingTimeout.add(proc);
          }
        } else {
          // TODO: If not executable put back in the queue.
          // we should have the lock-queue and add that back to the list to
          // avoid all the try-lock failures.
          waitingTimeout.add(proc);
        }
        continue;
      }

      // The procedure received an "abort-timeout", call abort() and
      // add the procedure back in the queue for rollback.
      if (proc.setTimeoutFailure()) {
        long rootProcId = Procedure.getRootProcedureId(procedures, proc);
        ProcedureStack procStack = rollbackStack.get(rootProcId);
        procStack.abort();
        store.update(proc);
        runnables.addFront(proc);
        continue;
      }
    }
  }

  /**
   * Execute the rollback of the full procedure stack.
   * Once the procedure is rolledback, the root-procedure will be visible as
   * finished to user, and the result will be the fatal exception.
   */
  private boolean executeRollback(final long rootProcId, final ProcedureStack procStack) {
    Procedure rootProc = procedures.get(rootProcId);
    RemoteProcedureException exception = rootProc.getException();
    if (exception == null) {
      exception = procStack.getException();
      rootProc.setFailure(exception);
      store.update(rootProc);
    }

    List<Procedure> subprocStack = procStack.getSubprocedures();
    int stackTail = subprocStack.size();
    while (stackTail --> 0) {
      Procedure proc = subprocStack.remove(stackTail);
      if (proc.acquireLock(getEnvironment())) {
        executeRollback(proc);
        proc.releaseLock(getEnvironment());
      } else {
        // can't take a lock on the procedure, add the root-proc back on the
        // queue waiting for the lock availability
        runnables.yield(rootProc);
        return false;
      }

      // allows to kill the executor before something is stored to the wal.
      // useful to test the procedure recovery.
      if (testing != null && testing.killBeforeStoreUpdate) {
        stop();
        return false;
      }
    }

    // Finalize the procedure state
    completed.put(rootProcId, newResultFromProcedure(rootProc));
    rollbackStack.remove(rootProcId);
    LOG.info("Rolledback procedure " + rootProc + " exception: " + exception);
    return true;
  }

  /**
   * Execute the rollback of the procedure step.
   * It updates the store with the new state (stack index)
   * or will remove completly the procedure in case it is a child.
   */
  private void executeRollback(final Procedure proc) {
    proc.doRollback(getEnvironment());

    // allows to kill the executor before something is stored to the wal.
    // useful to test the procedure recovery.
    if (testing != null && testing.killBeforeStoreUpdate) {
      stop();
      return;
    }

    if (proc.removeStackIndex()) {
      proc.setState(ProcedureState.ROLLEDBACK);
      if (proc.hasParent()) {
        store.delete(proc.getProcId());
        procedures.remove(proc.getProcId());
      } else {
        store.update(proc);
      }
    } else {
      store.update(proc);
    }
  }

  /**
   * Executes the specified procedure
   *  - calls the doExecute() of the procedure
   *  - if the procedure execution didn't fail (e.g. invalid user input)
   *     - ...and returned subprocedures
   *        - the subprocedures are initialized.
   *        - the subprocedures are added to the store
   *        - the subprocedures are added to the runnable queue
   *        - the procedure is now in a WAITING state, waiting for the subprocedures to complete
   *     - ...if there are no subprocedure
   *        - the procedure completed successfully
   *        - if there is a parent (WAITING)
   *            - the parent state will be set to RUNNABLE
   *  - in case of failure
   *    - the store is updated with the new state
   *    - the executor (caller of this method) will start the rollback of the procedure
   */
  private void execProcedure(final ProcedureStack procStack, final Procedure procedure) {
    assert procedure.getState() == ProcedureState.RUNNABLE;

    // Execute the procedure
    boolean reExecute = false;
    Procedure[] subprocs = null;
    do {
      reExecute = false;
      try {
        subprocs = procedure.doExecute(getEnvironment());
        if (subprocs != null && subprocs.length == 0) {
          subprocs = null;
        }
      } catch (Exception e) {
        // Catch NullPointerExceptions or similar errors...
        procedure.setFailure(new RemoteProcedureException("uncatched runtime exception", e));
      }

      if (!procedure.isFailed()) {
        if (subprocs != null) {
          if (subprocs.length == 1 && subprocs[0] == procedure) {
            // quick-shortcut for a state machine like procedure
            subprocs = null;
            reExecute = true;
          } else {
            // yield the current procedure, and make the subprocedure runnable
            for (int i = 0; i < subprocs.length; ++i) {
              Procedure subproc = subprocs[i];
              if (subproc == null) {
                String msg = "subproc[" + i + "] is null, aborting the procedure";
                procedure.setFailure(new RemoteProcedureException(msg,
                  new IllegalArgumentException(msg)));
                subprocs = null;
                break;
              }

              assert subproc.getState() == ProcedureState.INITIALIZING;
              subproc.setParentProcId(procedure.getProcId());
              subproc.setProcId(lastProcId.incrementAndGet());
            }

            if (!procedure.isFailed()) {
              procedure.setChildrenLatch(subprocs.length);
              switch (procedure.getState()) {
                case RUNNABLE:
                  procedure.setState(ProcedureState.WAITING);
                  break;
                case WAITING_TIMEOUT:
                  waitingTimeout.add(procedure);
                  break;
              }
            }
          }
        } else if (procedure.getState() == ProcedureState.WAITING_TIMEOUT) {
          waitingTimeout.add(procedure);
        } else {
          // No subtask, so we are done
          procedure.setState(ProcedureState.FINISHED);
        }
      }

      // Add the procedure to the stack
      procStack.addRollbackStep(procedure);

      // allows to kill the executor before something is stored to the wal.
      // useful to test the procedure recovery.
      if (testing != null && testing.killBeforeStoreUpdate) {
        stop();
        return;
      }

      // Commit the transaction
      if (subprocs != null && !procedure.isFailed()) {
        store.insert(procedure, subprocs);
      } else {
        store.update(procedure);
      }

      assert (reExecute && subprocs == null) || !reExecute;
    } while (reExecute);

    // Submit the new subprocedures
    if (subprocs != null && !procedure.isFailed()) {
      for (int i = 0; i < subprocs.length; ++i) {
        Procedure subproc = subprocs[i];
        assert !procedures.containsKey(subproc.getProcId());
        procedures.put(subproc.getProcId(), subproc);
        runnables.addFront(subproc);
      }
    }

    if (procedure.isFinished() && procedure.hasParent()) {
      Procedure parent = procedures.get(procedure.getParentProcId());
      if (parent == null) {
        assert procStack.isRollingback();
        return;
      }

      // If this procedure is the last child awake the parent procedure
      if (parent.childrenCountDown() && parent.getState() == ProcedureState.WAITING) {
        parent.setState(ProcedureState.RUNNABLE);
        store.update(parent);
        runnables.addFront(parent);
        return;
      }
    }
  }

  private Long getRootProcedureId(Procedure proc) {
    return Procedure.getRootProcedureId(procedures, proc);
  }

  private static ProcedureResult newResultFromProcedure(final Procedure proc) {
    if (proc.isFailed()) {
      return new ProcedureResult(proc.getLastUpdate(), proc.getException());
    }
    return new ProcedureResult(proc.getLastUpdate(), proc.getResult());
  }
}