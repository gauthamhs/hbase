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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.procedure2.ProcedureResult;
import org.apache.hadoop.hbase.procedure2.TimedEventProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStore;
import org.apache.hadoop.hbase.procedure2.util.TimeoutBlockingQueue;
import org.apache.hadoop.hbase.procedure2.util.TimeoutBlockingQueue.TimeoutRetriever;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProcedureExecutor {
  private static final Log LOG = LogFactory.getLog(ProcedureExecutor.class);

  static Testing testing = null;
  public static class Testing {
    public boolean killBeforeStoreUpdate = false;
  }

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

  private static class CompletedProcedureCleaner
      extends Procedure implements TimedEventProcedure {
    private static final Log LOG = LogFactory.getLog(CompletedProcedureCleaner.class);

    private static final long EVICT_TTL = 5 * 60000; // 5min

    private final Map<Long, ProcedureResult> completed;
    private final ProcedureStore store;

    public CompletedProcedureCleaner(final ProcedureStore store,
        final Map<Long, ProcedureResult> completedMap) {
      this.completed = completedMap;
      this.store = store;
      setTimeout(5000);
    }

    @Override
    public Procedure[] execute() {
      LOG.debug("Compled Procedure Cleaner, completed " + completed.size());
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
      return null;
    }

    @Override
    public void rollback() {
      // no-op
    }

    @Override
    public void abort() {
      // no-op
    }

    @Override
    public void serializeStateData(final OutputStream stream) {
      // no-op
    }

    @Override
    public void deserializeStateData(final InputStream stream) {
      // no-op
    }
  }

  private final AtomicInteger activeExecutorCount = new AtomicInteger(0);
  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  private final ConcurrentHashMap<Long, ProcedureResult> completed =
    new ConcurrentHashMap<Long, ProcedureResult>();
  private final ConcurrentHashMap<Long, ProcedureStack> rollbackStack =
    new ConcurrentHashMap<Long, ProcedureStack>();
  private final ConcurrentHashMap<Long, Procedure> procedures =
    new ConcurrentHashMap<Long, Procedure>();
  private final TimeoutBlockingQueue<Procedure> waitingTimeout =
    new TimeoutBlockingQueue<Procedure>(new ProcedureTimeoutRetriever());

  private final ProcedureRunnableSet runnables = new ProcedureSingleRunQueue();
  private final AtomicLong lastProcId = new AtomicLong(0);
  private final ProcedureStore store;

  private Thread[] threads;

  public ProcedureExecutor(final ProcedureStore store) {
    this.store = store;
  }

  public List<Map.Entry<Long, ProcedureStack>> load() throws IOException {
    assert completed.isEmpty();
    assert rollbackStack.isEmpty();
    assert procedures.isEmpty();
    assert waitingTimeout.isEmpty();
    assert runnables.size() == 0;

    // 1. Load the procedures
    Iterator<Procedure> loader = store.load();
    if (loader == null) return null;

    long logMaxProcId = 0;
    int runnablesCount = 0;
    while (loader.hasNext()) {
      Procedure proc = loader.next();
      procedures.put(proc.getProcId(), proc);
      logMaxProcId = Math.max(logMaxProcId, proc.getProcId());
      if (!proc.hasParent() && !proc.isFinished()) {
        rollbackStack.put(proc.getProcId(), new ProcedureStack());
      }
      if (proc.getState() == ProcedureState.RUNNABLE) {
        runnablesCount++;
      }
    }
    assert lastProcId.get() == 0;
    lastProcId.set(logMaxProcId + 1);

    // 2. Initialize the stacks
    HashSet<Procedure> runnableSet = null;
    HashSet<Procedure> waitingSet = null;
    for (final Procedure proc: procedures.values()) {
      Long rootProcId = getRootProcedureId(proc);
      if (rootProcId == null) {
        // The 'proc' was ready to run but the root procedure was rolledback
        runnables.add(proc);
        continue;
      }

      if (!proc.hasParent() && proc.isFinished()) {
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

        case ROLLEDBACK:
        case FINISHED:
        case INITIALIZING:
          throw new UnsupportedOperationException("Unexpected " + proc.getState() +
            " state for " + proc);
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
        runnables.add(proc);
      }
    }
    return corrupted;
  }

  public void start(int numThreads) throws IOException {
    if (isRunning.getAndSet(true)) {
      LOG.warn("Already running");
      return;
    }

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

    // Initialize procedures timeout handler
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

    for (int i = 0; i < threads.length; ++i) {
      threads[i].start();
    }

    // Add completed cleaner
    waitingTimeout.add(new CompletedProcedureCleaner(store, completed));
  }

  public void stop() {
    isRunning.set(false);
    runnables.signalAll();
    waitingTimeout.signalAll();
  }

  public boolean isRunning() {
    return isRunning.get();
  }

  public int getActiveExecutorCount() {
    return activeExecutorCount.get();
  }

  public long submitProcedure(final Procedure proc) {
    assert proc.getState() == ProcedureState.INITIALIZING;

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
    runnables.add(proc);
    return proc.getProcId();
  }

  public Map<Long, ProcedureResult> getResults() {
    return completed;
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
      proc.abort();
      return true;
    }
    return false;
  }

  private void execLoop() {
    while (isRunning.get()) {
      Long procId = runnables.poll();
      Procedure proc = procId != null ? procedures.get(procId) : null;
      if (proc == null) continue;

      activeExecutorCount.incrementAndGet();
      try {
        Long rootProcId = getRootProcedureId(proc);
        if (rootProcId == null) {
          // The 'proc' was ready to run but the root procedure was rolledback
          executeRollback(proc);
          continue;
        }

        ProcedureStack procStack = rollbackStack.get(rootProcId);
        while (proc != null || procStack.isFailed()) {
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
          Procedure nextProc = execProcedure(procStack, proc);
          procStack.release(proc);
          if (proc.getProcId() == rootProcId && proc.isCompleted()) {
            assert nextProc == null;

            // Finalize the procedure state
            completed.put(rootProcId, newResultFromProcedure(proc));
            rollbackStack.remove(rootProcId);
            LOG.info("Procedure completed: " + proc);
            break;
          }

          // continue, with the selected procedure
          proc = nextProc;
        }
      } finally {
        activeExecutorCount.decrementAndGet();
      }
    }
  }

  private void timeoutLoop() {
    while (isRunning.get()) {
      Procedure proc = waitingTimeout.poll();
      if (proc == null) continue;

      if (proc.getMillisToTimeout() > 0) {
        // got an early wake, maybe a stop?
        // re-enqueue the task in case was not a stop or just a signal
        waitingTimeout.add(proc);
        continue;
      }

      // TimedEventProcedure are not real procedures they are more like java Timers
      if (proc instanceof TimedEventProcedure) {
        // TODO-MAYBE: Should we provide a notification to the store with the
        // full set of procedures pending and completed to write a compacted
        // version of the log (in case is a log)?
        // In theory no, procedures are have a short life, so at some point the store
        // will have the tracker saying everything is in the last log.
        proc.doExecute();
        if (!proc.isFinished()) {
          waitingTimeout.add(proc);
        }
        continue;
      }

      if (proc.setTimeoutFailure()) {
        store.update(proc);
        runnables.add(proc);
        continue;
      }

      // TODO: Execute Procedure Timeout
    }
  }

  private void executeRollback(final long rootProcId, final ProcedureStack procStack) {
    List<Procedure> subprocStack = procStack.getSubprocedures();
    int stackTail = subprocStack.size();
    ForeignException exception = null;
    while (stackTail --> 1) {
      Procedure proc = subprocStack.remove(stackTail);
      ForeignException procException = proc.getException();
      if (exception == null && procException != null) {
        exception = procException;
      }
      executeRollback(proc);
    }

    // TODO
    assert stackTail == 0;
    Procedure proc = subprocStack.remove(0);
    proc.doRollback();
    proc.setFailure(exception);
    store.update(proc);

    // Finalize the procedure state
    completed.put(rootProcId, newResultFromProcedure(proc));
    rollbackStack.remove(rootProcId);
    LOG.info("Rolledback procedure " + proc);
  }

  private void executeRollback(final Procedure proc) {
    assert proc.hasParent();
    proc.doRollback();
    proc.setState(ProcedureState.ROLLEDBACK);
    store.delete(proc.getProcId());
    procedures.remove(proc.getProcId());
  }

  private Procedure execProcedure(final ProcedureStack procStack, final Procedure procedure) {
    assert procedure.getState() == ProcedureState.RUNNABLE;

    // Execute the procedure
    Procedure[] subprocs = null;
    try {
      subprocs = procedure.doExecute();
      if (subprocs != null && subprocs.length == 0) {
        subprocs = null;
      }
    } catch (Exception e) {
      // Catch NullPointerExceptions or similar errors...
      procedure.setFailure(new ForeignException("uncatched runtime exception", e));
    }

    if (!procedure.isFailed()) {
      if (subprocs != null) {
        // yield the current procedure, and make the subprocedure runnable
        for (int i = 0; i < subprocs.length; ++i) {
          Procedure subproc = subprocs[i];
          if (subproc == null) {
            String msg = "subproc[" + i + "] is null, aborting the procedure";
            procedure.setFailure(new ForeignException(msg,
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
      } else {
        // No subtask, so we are done
        procedure.setState(ProcedureState.FINISHED);
      }
    }

    // Add the procedure to the stack
    procStack.addRollbackStep(procedure);

    // allows to kill the executor before something is stored to the wal.
    // useful to test the procedure recovery.
    if (testing != null) {
      if (testing.killBeforeStoreUpdate) {
        stop();
        return null;
      }
    }

    // Commit the transaction
    if (subprocs != null && !procedure.isFailed()) {
      store.insert(procedure, subprocs);
    } else {
      store.update(procedure);
    }

    // Submit the new subprocedures
    if (subprocs != null && !procedure.isFailed()) {
      for (int i = 0; i < subprocs.length; ++i) {
        Procedure subproc = subprocs[i];
        assert !procedures.containsKey(subproc.getProcId());
        procedures.put(subproc.getProcId(), subproc);
        runnables.add(subproc);
      }
    }

    if (procedure.isFinished() && procedure.hasParent()) {
      Procedure parent = procedures.get(procedure.getParentProcId());
      if (parent == null) {
        assert procStack.isRollingback();
        return null;
      }

      // If this procedure is the last child awake the parent procedure
      if (parent.childrenCountDown()) {
        if (parent.getState() != ProcedureState.WAITING) {
          LOG.warn("Unexpected parent state, should have been WAITING got " +
                   parent.getState() + ": " + procedure);
        }

        parent.setState(ProcedureState.RUNNABLE);
        store.update(parent);
        return parent;
      }
    }
    return null;
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